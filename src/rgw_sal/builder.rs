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

//! Builder for SAL-based object store

use std::sync::Arc;

use url::Url;

use crate::{Error, ObjectStore, Path, Result};

use super::{SalClient, SalObjectStore};

/// Configuration options for SAL object store
#[derive(Debug, Clone, Default)]
pub struct SalBuilder {
    /// Ceph cluster name (default: "ceph")
    cluster_name: Option<String>,
    /// Ceph user name (default: "admin")
    user_name: Option<String>,
    /// Path to ceph.conf file
    conf_file: Option<String>,
    /// Bucket name (required)
    bucket_name: Option<String>,
    /// Tenant name for multi-tenancy
    tenant: Option<String>,
    /// Path to keyring file
    keyring: Option<String>,
    /// Authentication key
    key: Option<String>,
}

impl SalBuilder {
    /// Create a new builder with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the cluster name (default: "ceph")
    pub fn with_cluster_name(mut self, cluster_name: impl Into<String>) -> Self {
        self.cluster_name = Some(cluster_name.into());
        self
    }

    /// Set the user name (default: "admin")
    pub fn with_user_name(mut self, user_name: impl Into<String>) -> Self {
        self.user_name = Some(user_name.into());
        self
    }

    /// Set the path to ceph.conf
    pub fn with_conf_file(mut self, conf_file: impl Into<String>) -> Self {
        self.conf_file = Some(conf_file.into());
        self
    }

    /// Set the bucket name (required)
    pub fn with_bucket(mut self, bucket_name: impl Into<String>) -> Self {
        self.bucket_name = Some(bucket_name.into());
        self
    }

    /// Set the tenant name for multi-tenancy
    pub fn with_tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = Some(tenant.into());
        self
    }

    /// Set the keyring file path for authentication
    pub fn with_keyring(mut self, keyring: impl Into<String>) -> Self {
        self.keyring = Some(keyring.into());
        self
    }

    /// Set the authentication key directly
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Build the SAL object store
    pub fn build(self) -> Result<SalObjectStore> {
        let bucket_name = self.bucket_name.ok_or_else(|| Error::Generic {
            store: "SAL",
            source: "bucket_name is required".into(),
        })?;

        let cluster_name = self.cluster_name.unwrap_or_else(|| "ceph".to_string());
        let user_name = self.user_name.unwrap_or_else(|| "admin".to_string());

        let client = SalClient::new(
            cluster_name,
            user_name,
            self.conf_file,
            bucket_name,
            self.tenant,
            self.keyring,
            self.key,
        )?;

        Ok(SalObjectStore {
            client: Arc::new(client),
        })
    }

    /// Build from environment variables
    ///
    /// Supported environment variables:
    /// - `CEPH_CLUSTER_NAME` - Cluster name (default: "ceph")
    /// - `CEPH_USER_NAME` - User name (default: "admin")
    /// - `CEPH_CONF_FILE` - Path to ceph.conf
    /// - `CEPH_BUCKET_NAME` - Bucket name (required)
    /// - `CEPH_TENANT` - Tenant name
    /// - `CEPH_KEYRING` - Path to keyring file
    /// - `CEPH_KEY` - Authentication key
    pub fn from_env() -> Result<Self> {
        let mut builder = Self::new();

        if let Ok(cluster_name) = std::env::var("CEPH_CLUSTER_NAME") {
            builder = builder.with_cluster_name(cluster_name);
        }

        if let Ok(user_name) = std::env::var("CEPH_USER_NAME") {
            builder = builder.with_user_name(user_name);
        }

        if let Ok(conf_file) = std::env::var("CEPH_CONF_FILE") {
            builder = builder.with_conf_file(conf_file);
        }

        if let Ok(bucket_name) = std::env::var("CEPH_BUCKET_NAME") {
            builder = builder.with_bucket(bucket_name);
        }

        if let Ok(tenant) = std::env::var("CEPH_TENANT") {
            builder = builder.with_tenant(tenant);
        }

        if let Ok(keyring) = std::env::var("CEPH_KEYRING") {
            builder = builder.with_keyring(keyring);
        }

        if let Ok(key) = std::env::var("CEPH_KEY") {
            builder = builder.with_key(key);
        }

        Ok(builder)
    }

    /// Build from a URL
    ///
    /// URL format: `sal://[cluster@]bucket[/path]?param=value`
    ///
    /// Supported query parameters:
    /// - `user` - User name
    /// - `conf` - Path to ceph.conf
    /// - `tenant` - Tenant name
    /// - `keyring` - Path to keyring file
    /// - `key` - Authentication key
    ///
    /// Examples:
    /// - `sal://mybucket` - Use default cluster and admin user
    /// - `sal://mycluster@mybucket` - Use specific cluster
    /// - `sal://mybucket?user=myuser&conf=/etc/ceph/ceph.conf`
    pub fn from_url(url: &Url) -> Result<Self> {
        let bucket_name = url.host_str().ok_or_else(|| Error::Generic {
            store: "SAL",
            source: "Missing bucket name in URL".into(),
        })?;

        let mut builder = Self::new().with_bucket(bucket_name);

        // Extract cluster name from username if present
        if let Some(username) = url.username() {
            if !username.is_empty() {
                builder = builder.with_cluster_name(username);
            }
        }

        // Parse query parameters
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "user" => builder = builder.with_user_name(value.into_owned()),
                "conf" => builder = builder.with_conf_file(value.into_owned()),
                "tenant" => builder = builder.with_tenant(value.into_owned()),
                "keyring" => builder = builder.with_keyring(value.into_owned()),
                "key" => builder = builder.with_key(value.into_owned()),
                _ => {
                    // Ignore unknown parameters
                }
            }
        }

        Ok(builder)
    }
}

/// SAL-based ObjectStore implementation
#[derive(Debug, Clone)]
pub struct SalObjectStore {
    pub(crate) client: Arc<SalClient>,
}

impl std::fmt::Display for SalObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SalObjectStore(bucket={})", self.client.bucket_name())
    }
}

#[async_trait::async_trait]
impl ObjectStore for SalObjectStore {
    async fn put(&self, location: &Path, payload: crate::PutPayload) -> Result<crate::PutResult> {
        self.client.put_opts(location, payload, crate::PutOptions::default()).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: crate::PutPayload,
        opts: crate::PutOptions,
    ) -> Result<crate::PutResult> {
        self.client.put_opts(location, payload, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<Box<dyn crate::MultipartUpload>> {
        self.client.put_multipart(location, crate::PutMultipartOptions::default()).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: crate::PutMultipartOptions,
    ) -> Result<Box<dyn crate::MultipartUpload>> {
        self.client.put_multipart(location, opts).await
    }

    async fn get(&self, location: &Path) -> Result<crate::GetResult> {
        self.client.get_opts(location, crate::GetOptions::default()).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: crate::GetOptions,
    ) -> Result<crate::GetResult> {
        self.client.get_opts(location, options).await
    }

    async fn get_range(
        &self,
        location: &Path,
        range: std::ops::Range<usize>,
    ) -> Result<bytes::Bytes> {
        let opts = crate::GetOptions {
            range: Some(crate::GetRange::Bounded(range.clone())),
            ..Default::default()
        };
        let result = self.client.get_opts(location, opts).await?;
        result.bytes().await
    }

    async fn head(&self, location: &Path) -> Result<crate::ObjectMeta> {
        let result = self.get(location).await?;
        Ok(result.meta)
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let stream = futures::stream::once(async { Ok(location.clone()) }).boxed();
        let mut result_stream = self.client.delete_stream(stream).await;

        // Wait for the delete to complete
        use futures::StreamExt;
        while let Some(result) = result_stream.next().await {
            result?;
        }

        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> futures::stream::BoxStream<'_, Result<crate::ObjectMeta>> {
        match self.client.list(prefix) {
            Ok(stream) => stream,
            Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<crate::ListResult> {
        use futures::StreamExt;

        let stream = self.list(prefix);
        let objects: Vec<_> = stream.collect().await;

        let mut common_prefixes = Vec::new();
        let mut objects_vec = Vec::new();

        for obj_result in objects {
            match obj_result {
                Ok(obj) => objects_vec.push(obj),
                Err(e) => return Err(e),
            }
        }

        Ok(ListResult {
            common_prefixes,
            objects: objects_vec,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        // SAL doesn't have a native copy operation, so we do get + put
        let data = self.get(from).await?;
        let bytes = data.bytes().await?;
        self.put(to, crate::PutPayload::from(bytes)).await?;
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        // Check if destination exists
        if self.head(to).await.is_ok() {
            return Err(Error::AlreadyExists {
                path: to.to_string(),
                source: "Destination already exists".into(),
            });
        }

        self.copy(from, to).await
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
    fn test_from_url() {
        let url = Url::parse("sal://mybucket?user=myuser&conf=/etc/ceph/ceph.conf").unwrap();
        let builder = SalBuilder::from_url(&url).unwrap();

        assert_eq!(builder.bucket_name, Some("mybucket".to_string()));
        assert_eq!(builder.user_name, Some("myuser".to_string()));
        assert_eq!(builder.conf_file, Some("/etc/ceph/ceph.conf".to_string()));
    }

    #[test]
    fn test_from_url_with_cluster() {
        let url = Url::parse("sal://mycluster@mybucket").unwrap();
        let builder = SalBuilder::from_url(&url).unwrap();

        assert_eq!(builder.bucket_name, Some("mybucket".to_string()));
        assert_eq!(builder.cluster_name, Some("mycluster".to_string()));
    }
}
