use std::time::{Duration, SystemTime};
use std::{fmt, io};

use anyhow::{Context, Result};
use futures_util::{StreamExt, TryStreamExt};
use objectstore_types::{ExpirationPolicy, Metadata};
use reqwest::{Body, Certificate, IntoUrl, Method, RequestBuilder, StatusCode};

use crate::backend::common::{self, Backend, BackendStream};
use crate::path::ObjectPath;

/// Prefix used for custom metadata in headers for the GCS backend.
///
/// See: <https://cloud.google.com/storage/docs/xml-api/reference-headers#xgoogmeta>
const GCS_CUSTOM_PREFIX: &str = "x-goog-meta-";
/// Header used to store the expiration time for GCS using the `daysSinceCustomTime` lifecycle
/// condition.
///
/// See: <https://cloud.google.com/storage/docs/xml-api/reference-headers#xgoogcustomtime>
const GCS_CUSTOM_TIME: &str = "x-goog-custom-time";
/// Time to debounce bumping an object with configured TTI.
const TTI_DEBOUNCE: Duration = Duration::from_secs(24 * 3600); // 1 day

pub trait Token: Send + Sync {
    fn as_str(&self) -> &str;
}

pub trait TokenProvider: Send + Sync + 'static {
    fn get_token(&self) -> impl Future<Output = Result<impl Token>> + Send;
}

// this only exists because we have to provide *some* kind of provider
#[derive(Debug)]
pub struct NoToken;

impl TokenProvider for NoToken {
    #[allow(refining_impl_trait_internal)] // otherwise, returning `!` will not implement the required traits
    async fn get_token(&self) -> Result<NoToken> {
        unimplemented!()
    }
}
impl Token for NoToken {
    fn as_str(&self) -> &str {
        unimplemented!()
    }
}

pub struct S3CompatibleBackend<T> {
    client: reqwest::Client,

    endpoint: String,
    bucket: String,

    token_provider: Option<T>,
}

impl<T> S3CompatibleBackend<T> {
    /// Creates a new S3 compatible backend bound to the given bucket.
    #[expect(dead_code)]
    pub fn new(endpoint: &str, bucket: &str, token_provider: T) -> Self {
        Self {
            client: common::reqwest_client(),
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            token_provider: Some(token_provider),
        }
    }

    /// Formats the S3 object URL for the given key.
    fn object_url(&self, path: &ObjectPath) -> String {
        format!("{}/{}/{path}", self.endpoint, self.bucket)
    }
}

impl<T> S3CompatibleBackend<T>
where
    T: TokenProvider,
{
    /// Creates a request builder with the appropriate authentication.
    async fn request(&self, method: Method, url: impl IntoUrl) -> Result<RequestBuilder> {
        let mut builder = self.client.request(method, url);
        if let Some(provider) = &self.token_provider {
            builder = builder.bearer_auth(provider.get_token().await?.as_str());
        }
        Ok(builder)
    }

    /// Issues a request to update the metadata for the given object.
    async fn update_metadata(&self, path: &ObjectPath, metadata: &Metadata) -> Result<()> {
        // NB: Meta updates require copy + REPLACE along with *all* metadata. See
        // https://cloud.google.com/storage/docs/xml-api/put-object-copy
        self.request(Method::PUT, self.object_url(path))
            .await?
            .header("x-goog-copy-source", format!("/{}/{path}", self.bucket))
            .header("x-goog-metadata-directive", "REPLACE")
            .headers(metadata.to_headers(GCS_CUSTOM_PREFIX, true)?)
            .send()
            .await?
            .error_for_status()
            .context("failed to update expiration time for object with TTI")?;

        Ok(())
    }
}

impl<T> fmt::Debug for S3CompatibleBackend<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Compatible")
            .field("client", &self.client)
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

impl S3CompatibleBackend<NoToken> {
    pub fn without_token(endpoint: &str, bucket: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            token_provider: None,
        }
    }
}

#[async_trait::async_trait]
impl<T: TokenProvider> Backend for S3CompatibleBackend<T> {
    fn name(&self) -> &'static str {
        "s3-compatible"
    }

    #[tracing::instrument(level = "trace", fields(?path), skip_all)]
    async fn put_object(
        &self,
        path: &ObjectPath,
        metadata: &Metadata,
        stream: BackendStream,
    ) -> Result<()> {
        tracing::debug!("Writing to s3_compatible backend");
        self.request(Method::PUT, self.object_url(path))
            .await?
            .headers(metadata.to_headers(GCS_CUSTOM_PREFIX, true)?)
            .body(Body::wrap_stream(stream))
            .send()
            .await?
            .error_for_status()
            .context("failed to put object")?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", fields(?path), skip_all)]
    async fn get_object(&self, path: &ObjectPath) -> Result<Option<(Metadata, BackendStream)>> {
        tracing::debug!("Reading from s3_compatible backend");
        let object_url = self.object_url(path);

        let response = self.request(Method::GET, &object_url).await?.send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            tracing::debug!("Object not found");
            return Ok(None);
        }

        let response = response
            .error_for_status()
            .context("failed to get object")?;

        let headers = response.headers();
        // TODO: Populate size in metadata
        let metadata = Metadata::from_headers(headers, GCS_CUSTOM_PREFIX)?;

        // TODO: Schedule into background persistently so this doesn't get lost on restarts
        if let ExpirationPolicy::TimeToIdle(tti) = metadata.expiration_policy {
            // TODO: Inject the access time from the request.
            let access_time = SystemTime::now();

            let expire_at = headers
                .get(GCS_CUSTOM_TIME)
                .and_then(|s| s.to_str().ok())
                .and_then(|s| humantime::parse_rfc3339(s).ok())
                .unwrap_or(access_time);

            if expire_at < access_time + tti - TTI_DEBOUNCE {
                // This serializes a new custom-time internally.
                self.update_metadata(path, &metadata).await?;
            }
        }

        // TODO: the object *GET* should probably also contain the expiration time?

        let stream = response.bytes_stream().map_err(io::Error::other);
        Ok(Some((metadata, stream.boxed())))
    }

    #[tracing::instrument(level = "trace", fields(?path), skip_all)]
    async fn delete_object(&self, path: &ObjectPath) -> Result<()> {
        tracing::debug!("Deleting from s3_compatible backend");
        let response = self
            .request(Method::DELETE, self.object_url(path))
            .await?
            .send()
            .await?;

        // Do not error for objects that do not exist.
        if response.status() != StatusCode::NOT_FOUND {
            tracing::debug!("Object not found");
            response
                .error_for_status()
                .context("failed to delete object")?;
        }

        Ok(())
    }
}

pub struct AWSv4Signer {
    region: String,
    path_style: bool,
    access_key_id: String,
    secret_access_key: String,
}

impl AWSv4Signer {
    pub fn new(
        region: String,
        path_style: bool,
        access_key_id: String,
        secret_access_key: String,
    ) -> Self {
        Self {
            region,
            path_style,
            access_key_id,
            secret_access_key,
        }
    }
}

impl TokenProvider for AWSv4Signer {
    async fn get_token(&self) -> Result<impl Token> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_str(
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            )
            .expect("invalid sha256 hash"),
        );
        headers.insert(
            "x-amz-date",
            HeaderValue::from_str("20150830T123600Z").expect("invalid date"),
        );
        headers.insert(
            "x-amz-security-token",
            HeaderValue::from_str("AQoDYXdzEPT//////////wEXAMPLEtc764bNrC9SAPBSM22wDOk4x4HIZ8j4FZTwdQWLWsKWHGBuFqwAeMicRXmxfpSPfIeoIYRqTflfKD8YUuwthAx7mSEI/qkPpKPi/kMcGdQrmGdeehM4IC1NtBmUpp2wUE8phUZampKsburEDy0KPkyQDYwT7WZ0wq5VSXDvp75YU9HFvlRd8Tx6q6fE8YQcHNVXAkiY9q6d+xo0rKwT38xVqr7ZD0u0iPPkUL64lIZbqBAz+scqKmlzm8FDrypNC9Yjc8fPOLn9FX9KSYvKTr4rvx3iSIlTJabIQwj2ICCR/oLxBA==")
                .expect("invalid security token"),
        );
        headers.insert(
            "x-amz-algorithm",
            HeaderValue::from_str("AWS4-HMAC-SHA256").expect("invalid algorithm"),
        );
        headers.insert(
            "x-amz-credential",
            HeaderValue::from_str(&format!(
                "{}/{}/s3/aws4_request",
                self.access_key_id, self.region
            ))
            .expect("invalid credential"),
        );

        let mut query = url::form_urlencoded::Serializer::new(String::new());
        query.append_pair("X-Amz-Algorithm", "AWS4-HMAC-SHA256");
        query.append_pair(
            "X-Amz-Credential",
            &format!("{}/{}/s3/aws4_request", self.access_key_id, self.region),
        );
        query.append_pair("X-Amz-Date", "20150830T123600Z");
        query.append_pair("X-Amz-Expires", "86400");
        query.append_pair("X-Amz-SignedHeaders", "host");
        query.append_pair(
            "X-Amz-Signature",
            "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
        );

        let url = format!(
            "https://s3.amazonaws.com/test-bucket/test-key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}/{}/s3/aws4_request&X-Amz-Date=20150830T123600Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        );

        Ok(AWSv4Token {
            url,
            headers,
            query,
        })
    }
}

impl Token for AWSv4Token {
    fn as_str(&self) -> &str {
        unimplemented!()
    }
}

pub struct TLSConfig {
    ca_certificate: Option<String>,
    skip_verify: bool,
}

impl Default for TLSConfig {
    fn default() -> Self {
        Self {
            ca_certificate: None,
            skip_verify: false,
        }
    }
}

impl S3CompatibleBackend<AWSv4Signer> {
    pub fn with_aws_v4_signer(
        endpoint: &str,
        bucket: &str,
        signer: AWSv4Signer,
        tls_config: TLSConfig,
    ) -> Self {
        let mut client_builder = reqwest::Client::builder();
        if let Some(ca_certificate) = tls_config.ca_certificate {
            let ca_certificate = Certificate::from_pem(ca_certificate.as_bytes());
            match ca_certificate {
                Ok(ca_certificate) => {
                    client_builder = client_builder.add_root_certificate(ca_certificate)
                }
                Err(err) => tracing::warn!("Failed to load CA certificate: {}", err),
            }
        }

        if tls_config.skip_verify {
            client_builder = client_builder.danger_accept_invalid_certs(true);
        }

        Self {
            client: client_builder.build().unwrap_or_default(),
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            token_provider: Some(signer),
        }
    }
}

impl<T> S3CompatibleBackend<T>
where
    T: AWSv4Signer,
{
    async fn request(&self, method: Method, url: impl IntoUrl) -> Result<RequestBuilder> {
        let mut builder = self.client.request(method, url);
        if let Some(provider) = &self.token_provider {
            builder = builder.bearer_auth(provider.get_token().await?.as_str());
        }
        Ok(builder)
    }
}
