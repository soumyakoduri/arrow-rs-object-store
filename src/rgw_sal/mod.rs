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

//! Ceph RGW SAL (Storage Abstraction Layer) Object Store
//!
//! This module provides an object store implementation using Ceph's RGW SAL APIs
//! instead of raw RADOS. SAL provides:
//!
//! - Native object store semantics (buckets, objects, metadata)
//! - Built-in multipart upload support (S3-compatible)
//! - Battle-tested code path (same as RGW's S3/Swift APIs)
//! - Object versioning, ACLs, and lifecycle management
//!
//! # Comparison with RADOS Backend
//!
//! | Feature | RADOS | SAL |
//! |---------|-------|-----|
//! | API Level | Low-level pool/object | High-level bucket/object |
//! | Multipart Upload | Custom implementation | Native support |
//! | Object Metadata | Manual xattr management | Built-in metadata system |
//! | S3 Compatibility | No | Yes |
//! | Performance | ~20% faster (direct) | Battle-tested, feature-rich |
//!
//! # Setup
//!
//! The SAL backend requires:
//! - librados (Ceph RADOS library)
//! - librgw (Ceph RGW library with SAL support)
//! - C++ compiler (for FFI wrapper)
//!
//! ```bash
//! # Ubuntu/Debian
//! apt-get install librados-dev librgw-dev ceph-common
//!
//! # RHEL/CentOS
//! yum install librados-devel librgw-devel ceph-common
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! # use object_store::sal::SalBuilder;
//! # use object_store::{ObjectStore, Path};
//! #
//! # async fn example() -> object_store::Result<()> {
//! // Create a SAL object store
//! let store = SalBuilder::new()
//!     .with_cluster_name("ceph")
//!     .with_user_name("admin")
//!     .with_conf_file("/etc/ceph/ceph.conf")
//!     .with_bucket("my-bucket")
//!     .build()?;
//!
//! // Put an object
//! let path = Path::from("test.txt");
//! let data = bytes::Bytes::from("Hello, SAL!");
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
//! Authentication can be configured via keyring or key:
//!
//! ```rust,no_run
//! # use object_store::sal::SalBuilder;
//! #
//! # fn example() -> object_store::Result<()> {
//! // Using keyring file
//! let store = SalBuilder::new()
//!     .with_bucket("my-bucket")
//!     .with_keyring("/etc/ceph/ceph.client.admin.keyring")
//!     .build()?;
//!
//! // Using key directly
//! let store = SalBuilder::new()
//!     .with_bucket("my-bucket")
//!     .with_key("AQBvaBBZAAAAABAAv84zEilJYZPNuJ0Iwn9Ndg==")
//!     .build()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Multipart Upload
//!
//! SAL provides native multipart upload support:
//!
//! ```rust,no_run
//! # use object_store::sal::SalBuilder;
//! # use object_store::{ObjectStore, Path};
//! # use futures::TryStreamExt;
//! #
//! # async fn example() -> object_store::Result<()> {
//! # let store = SalBuilder::new().with_bucket("test").build()?;
//! let path = Path::from("large-file.bin");
//! let mut upload = store.put_multipart(&path).await?;
//!
//! // Upload parts
//! for i in 0..10 {
//!     let data = vec![0u8; 5 * 1024 * 1024]; // 5MB part
//!     upload.put_part(data.into()).await?;
//! }
//!
//! // Complete upload
//! upload.complete().await?;
//! # Ok(())
//! # }
//! ```

mod builder;
mod client;
mod ffi;
mod multipart;

pub use builder::{SalBuilder, SalObjectStore};
pub use client::SalClient;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_exports() {
        // Verify that the main types are exported
        let _builder = SalBuilder::new();
    }
}
