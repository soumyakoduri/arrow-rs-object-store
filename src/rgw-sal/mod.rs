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

//! An object store implementation for Ceph RADOS
//!
//! This module provides an ObjectStore implementation for Ceph RADOS,
//! allowing interaction with Ceph object storage through the librados API.
//!
//! ## Example
//!
//! ```no_run
//! # use object_store::rados::RadosBuilder;
//! # async fn example() -> object_store::Result<()> {
//! let rados = RadosBuilder::new()
//!     .with_pool_name("my-pool")
//!     .with_cluster_name("ceph")
//!     .with_user_name("client.admin")
//!     .with_conf_file("/etc/ceph/ceph.conf")
//!     .build()?;
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;
use futures::stream::BoxStream;
use std::sync::Arc;

use crate::{
    GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore, Path, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, Result, MultipartUpload, MultipartId,
};

mod builder;
mod client;
mod multipart;

#[cfg(test)]
mod tests;

pub use builder::{RadosBuilder, RadosConfigKey};
pub(crate) use multipart::RadosMultipartUpload;

const STORE: &str = "RADOS";

/// Interface for Ceph RADOS object storage
#[derive(Debug, Clone)]
pub struct CephRados {
    client: Arc<client::RadosClient>,
}

impl std::fmt::Display for CephRados {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CephRados(pool: {})", self.client.pool_name())
    }
}

impl CephRados {
    /// Create a new CephRados instance
    pub(crate) fn new(client: Arc<client::RadosClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ObjectStore for CephRados {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.client.put_opts(location, payload, opts).await
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

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.client.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.client.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.client.copy_if_not_exists(from, to).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.client.put_multipart_opts(location, opts).await
    }
}
