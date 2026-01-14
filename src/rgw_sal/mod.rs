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

//! RGW SAL (Storage Abstraction Layer) Backend
//!
//! This module provides an ObjectStore implementation using Ceph's RGW SAL APIs.
//!
//! # Overview
//!
//! The SAL backend offers a high-level, S3-compatible interface to Ceph storage,
//! providing advantages over raw RADOS access:
//!
//! - ✅ **Native bucket semantics** - Buckets, objects, metadata (not just pools)
//! - ✅ **Built-in multipart uploads** - S3-compatible multipart support
//! - ✅ **Battle-tested code** - Same code path as RGW's S3/Swift APIs
//! - ✅ **Rich features** - Object versioning, ACLs, lifecycle management
//!
//! # Example
//!
//! ```no_run
//! use object_store::rgw_sal::SalBuilder;
//! use object_store::{ObjectStore, Path};
//! use bytes::Bytes;
//!
//! # async fn example() -> object_store::Result<()> {
//! // Create SAL object store
//! let store = SalBuilder::new()
//!     .with_cluster_name("ceph")
//!     .with_user_name("admin")
//!     .with_conf_file("/etc/ceph/ceph.conf")
//!     .with_bucket("my-bucket")
//!     .build()?;
//!
//! // Put an object
//! let path = Path::from("test.txt");
//! let data = Bytes::from("Hello, SAL!");
//! store.put(&path, data.into()).await?;
//!
//! // Get the object
//! let result = store.get(&path).await?;
//! let bytes = result.bytes().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Authentication
//!
//! The SAL backend supports multiple authentication methods:
//!
//! ## Using configuration file and keyring:
//!
//! ```no_run
//! # use object_store::rgw_sal::SalBuilder;
//! # async fn example() -> object_store::Result<()> {
//! let store = SalBuilder::new()
//!     .with_bucket("my-bucket")
//!     .with_conf_file("/etc/ceph/ceph.conf")
//!     .with_keyring("/etc/ceph/ceph.client.admin.keyring")
//!     .build()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Using environment variables:
//!
//! ```bash
//! export CEPH_CLUSTER_NAME=ceph
//! export CEPH_USER_NAME=admin
//! export CEPH_CONF_FILE=/etc/ceph/ceph.conf
//! export CEPH_BUCKET_NAME=my-bucket
//! ```
//!
//! ```no_run
//! # use object_store::rgw_sal::SalBuilder;
//! # async fn example() -> object_store::Result<()> {
//! let store = SalBuilder::from_env()?.build()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Using URL:
//!
//! ```no_run
//! # use object_store::rgw_sal::SalBuilder;
//! # use url::Url;
//! # async fn example() -> object_store::Result<()> {
//! // Basic: sal://bucket-name
//! let url = Url::parse("sal://my-bucket")?;
//! let store = SalBuilder::from_url(&url)?.build()?;
//!
//! // With parameters: sal://bucket?user=admin&conf=/etc/ceph/ceph.conf
//! let url = Url::parse("sal://my-bucket?user=admin&conf=/etc/ceph/ceph.conf")?;
//! let store = SalBuilder::from_url(&url)?.build()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Multipart Upload
//!
//! The SAL backend provides native S3-compatible multipart upload:
//!
//! ```no_run
//! # use object_store::rgw_sal::SalBuilder;
//! # use object_store::{ObjectStore, Path};
//! # async fn example() -> object_store::Result<()> {
//! # let store = SalBuilder::new().with_bucket("test").build()?;
//! let path = Path::from("large-file.bin");
//! let mut upload = store.put_multipart(&path).await?;
//!
//! // Upload parts (minimum 5MB per part except last)
//! for i in 0..10 {
//!     let part_data = vec![0u8; 5 * 1024 * 1024]; // 5MB
//!     upload.put_part(part_data.into()).await?;
//! }
//!
//! // Complete the upload
//! let result = upload.complete().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Architecture
//!
//! The SAL backend consists of multiple layers:
//!
//! ```text
//! ┌─────────────────────────────────┐
//! │  ObjectStore Trait (Rust)       │
//! └────────────┬────────────────────┘
//!              │
//!              ▼
//! ┌─────────────────────────────────┐
//! │  SalObjectStore                 │
//! │  - Builder, Client, Multipart   │
//! └────────────┬────────────────────┘
//!              │ (Rust FFI)
//!              ▼
//! ┌─────────────────────────────────┐
//! │  C++ Wrapper                    │
//! │  - FFI-compatible functions     │
//! └────────────┬────────────────────┘
//!              │ (C++ calls)
//!              ▼
//! ┌─────────────────────────────────┐
//! │  RGW SAL C++ API                │
//! │  - Driver, Bucket, Object       │
//! └────────────┬────────────────────┘
//!              │
//!              ▼
//! ┌─────────────────────────────────┐
//! │  RADOS Storage                  │
//! └─────────────────────────────────┘
//! ```
//!
//! # Requirements
//!
//! - Ceph development libraries (librados-dev, librgw-dev)
//! - C++17 compiler
//! - Build with `rgw-sal` feature enabled
//!
//! # Comparison with RADOS Backend
//!
//! | Feature | RADOS | SAL |
//! |---------|-------|-----|
//! | Abstraction Level | Pool + Object | Bucket + Object |
//! | Multipart Upload | Custom | Native S3-compatible |
//! | Metadata | Manual (xattrs) | Built-in |
//! | Versioning | Not supported | Built-in |
//! | Performance | ~20% faster | Battle-tested |
//! | Dependencies | librados (C) | librados + librgw (C++) |

mod builder;
mod client;
mod ffi;
mod multipart;

pub use builder::{SalBuilder, SalObjectStore};

// Re-export commonly used types for convenience
pub use crate::{Error, ObjectStore, Path, Result};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_exports() {
        // Ensure all public types are accessible
        let _builder: SalBuilder;
        let _store: Option<SalObjectStore> = None;
    }
}
