// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! SAL Builder and ObjectStore Implementation
//!
//! This module provides the builder pattern for configuring and creating SAL ObjectStore instances.

use super::{client::SalClient, multipart::SalMultipartUpload};
use crate::{
    path::Path, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
};
use async_trait::async_trait;
use futures::stream::BoxStream;
use std::fmt;
use std::sync::Arc;
use url::Url;

/// Builder for SAL ObjectStore
///
/// Provides a fluent API for configuring and creating SAL object storage instances.
///
/// # Examples
///
/// ```no_run
/// use object_store::rgw_sal::SalBuilder;
///
/// # async fn example() -> object_store::Result<()> {
/// let store = SalBuilder::new()
///     .with_cluster_name("ceph")
///     .with_user_name("admin")
///     .with_conf_file("/etc/ceph/ceph.conf")
///     .with_bucket("my-bucket")
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
pub struct SalBuilder {
    cluster_name: Option<String>,
    user_name: Option<String>,
    conf_file: Option<String>,
    bucket_name: Option<String>,
    tenant: Option<String>,
    keyring: Option<String>,
    key: Option<String>,
}

impl SalBuilder {
    /// Create a new builder with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the Ceph cluster name (default: "ceph")
    pub fn with_cluster_name(mut self, name: impl Into<String>) -> Self {
        self.cluster_name = Some(name.into());
        self
    }

    /// Set the Ceph user name (default: "admin")
    pub fn with_user_name(mut self, name: impl Into<String>) -> Self {
        self.user_name = Some(name.into());
        self
    }

    /// Set the path to ceph.conf file
    pub fn with_conf_file(mut self, path: impl Into<String>) -> Self {
        self.conf_file = Some(path.into());
        self
    }

    /// Set the bucket name (required)
    pub fn with_bucket(mut self, name: impl Into<String>) -> Self {
        self.bucket_name = Some(name.into());
        self
    }

    /// Set the tenant name
    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// Set the path to keyring file
    pub fn with_keyring(mut self, path: impl Into<String>) -> Self {
        self.keyring = Some(path.into());
        self
    }

    /// Set the authentication key directly
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Create builder from environment variables
    ///
    /// Reads the following environment variables:
    /// - `CEPH_CLUSTER_NAME` - Cluster name (default: "ceph")
    /// - `CEPH_USER_NAME` - User name (default: "admin")
    /// - `CEPH_CONF_FILE` - Path to ceph.conf
    /// - `CEPH_BUCKET_NAME` - Bucket name (required)
    /// - `CEPH_TENANT` - Tenant name
    /// - `CEPH_KEYRING` - Path to keyring file
    /// - `CEPH_KEY` - Authentication key
    pub fn from_env() -> Result<Self> {
        let cluster_name = std::env::var("CEPH_CLUSTER_NAME").ok();
        let user_name = std::env::var("CEPH_USER_NAME").ok();
        let conf_file = std::env::var("CEPH_CONF_FILE").ok();
        let bucket_name = std::env::var("CEPH_BUCKET_NAME").ok();
        let tenant = std::env::var("CEPH_TENANT").ok();
        let keyring = std::env::var("CEPH_KEYRING").ok();
        let key = std::env::var("CEPH_KEY").ok();

        Ok(Self {
            cluster_name,
            user_name,
            conf_file,
            bucket_name,
            tenant,
            keyring,
            key,
        })
    }

    /// Create builder from URL
    ///
    /// Supports the following URL formats:
    /// - `sal://bucket-name`
    /// - `sal://bucket-name?user=admin&conf=/etc/ceph/ceph.conf`
    /// - `sal://cluster@bucket-name`
    ///
    /// Query parameters:
    /// - `user` - User name
    /// - `conf` - Path to ceph.conf
    /// - `tenant` - Tenant name
    /// - `keyring` - Path to keyring file
    /// - `key` - Authentication key
    pub fn from_url(url: &Url) -> Result<Self> {
        if url.scheme() != "sal" {
            return Err(Error::Generic {
                store: "SAL",
                source: format!("Invalid URL scheme: {}", url.scheme()).into(),
            });
        }

        // Extract bucket name from host
        let bucket_name = url
            .host_str()
            .ok_or_else(|| Error::Generic {
                store: "SAL",
                source: "Missing bucket name in URL".into(),
            })?
            .to_string();

        // Extract cluster name from username field (if present)
        let cluster_name = url.username();
        let cluster_name = if !cluster_name.is_empty() {
            Some(cluster_name.to_string())
        } else {
            None
        };

        // Parse query parameters
        let mut user_name = None;
        let mut conf_file = None;
        let mut tenant = None;
        let mut keyring = None;
        let mut key = None;

        for (k, v) in url.query_pairs() {
            match k.as_ref() {
                "user" => user_name = Some(v.to_string()),
                "conf" => conf_file = Some(v.to_string()),
                "tenant" => tenant = Some(v.to_string()),
                "keyring" => keyring = Some(v.to_string()),
                "key" => key = Some(v.to_string()),
                _ => {}
            }
        }

        Ok(Self {
            cluster_name,
            user_name,
            conf_file,
            bucket_name: Some(bucket_name),
            tenant,
            keyring,
            key,
        })
    }

    /// Build the SAL ObjectStore
    pub fn build(self) -> Result<SalObjectStore> {
        let cluster_name = self.cluster_name.unwrap_or_else(|| "ceph".to_string());
        let user_name = self.user_name.unwrap_or_else(|| "admin".to_string());
        let bucket_name = self.bucket_name.ok_or_else(|| Error::Generic {
            store: "SAL",
            source: "Bucket name is required".into(),
        })?;

        let client = SalClient::new(
            cluster_name,
            user_name,
            self.conf_file,
            bucket_name,
        );

        Ok(SalObjectStore {
            client: Arc::new(client),
        })
    }
}

/// SAL ObjectStore
///
/// Implements the ObjectStore trait using RGW SAL (Storage Abstraction Layer) APIs.
#[derive(Debug, Clone)]
pub struct SalObjectStore {
    client: Arc<SalClient>,
}

impl fmt::Display for SalObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.client)
    }
}

#[async_trait]
impl ObjectStore for SalObjectStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        self.client.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        let upload = SalMultipartUpload::new(
            (*self.client).clone(),
            location.clone(),
        )
        .await?;

        Ok(Box::new(upload))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.client.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.client.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.client.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.client.list_with_delimiter(prefix).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder() {
        let builder = SalBuilder::new()
            .with_cluster_name("test-cluster")
            .with_user_name("test-user")
            .with_bucket("test-bucket");

        assert_eq!(builder.cluster_name, Some("test-cluster".to_string()));
        assert_eq!(builder.user_name, Some("test-user".to_string()));
        assert_eq!(builder.bucket_name, Some("test-bucket".to_string()));
    }

    #[test]
    fn test_from_url_simple() {
        let url = Url::parse("sal://my-bucket").unwrap();
        let builder = SalBuilder::from_url(&url).unwrap();

        assert_eq!(builder.bucket_name, Some("my-bucket".to_string()));
    }

    #[test]
    fn test_from_url_with_params() {
        let url = Url::parse("sal://my-bucket?user=admin&conf=/etc/ceph/ceph.conf").unwrap();
        let builder = SalBuilder::from_url(&url).unwrap();

        assert_eq!(builder.bucket_name, Some("my-bucket".to_string()));
        assert_eq!(builder.user_name, Some("admin".to_string()));
        assert_eq!(builder.conf_file, Some("/etc/ceph/ceph.conf".to_string()));
    }

    #[test]
    fn test_from_url_with_cluster() {
        let url = Url::parse("sal://mycluster@my-bucket").unwrap();
        let builder = SalBuilder::from_url(&url).unwrap();

        assert_eq!(builder.cluster_name, Some("mycluster".to_string()));
        assert_eq!(builder.bucket_name, Some("my-bucket".to_string()));
    }

    #[test]
    fn test_build_missing_bucket() {
        let builder = SalBuilder::new();
        let result = builder.build();

        assert!(result.is_err());
    }
}
