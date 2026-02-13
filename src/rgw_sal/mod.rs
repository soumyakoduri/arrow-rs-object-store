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
//! use object_store::rgw_sal::SalClient;
//! use object_store::{ObjectStore, Path};
//! use bytes::Bytes;
//!
//! # async fn example() -> object_store::Result<()> {
//! // Create SAL client
//! let client = SalClient::new(
//!     "ceph".to_string(),                          // cluster name
//!     "admin".to_string(),                         // user name
//!     Some("/etc/ceph/ceph.conf".to_string()),     // config file
//!     "my-bucket".to_string(),                     // bucket name
//! )?;
//!
//! // Put an object
//! let path = Path::from("test.txt");
//! let data = Bytes::from("Hello, SAL!");
//! client.put(&path, data.into()).await?;
//!
//! // Get the object
//! let result = client.get(&path).await?;
//! let bytes = result.bytes().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Authentication
//!
//! The SAL backend uses Ceph configuration for authentication:
//!
//! ```no_run
//! # use object_store::rgw_sal::SalClient;
//! # async fn example() -> object_store::Result<()> {
//! let client = SalClient::new(
//!     "ceph".to_string(),
//!     "admin".to_string(),
//!     Some("/etc/ceph/ceph.conf".to_string()),
//!     "my-bucket".to_string(),
//! )?;
//! # Ok(())
//! # }
//! ```
//!
//! The configuration file and keyring follow standard Ceph conventions.
//! Authentication is handled by the underlying Ceph libraries.
//!
//! # Multipart Upload
//!
//! The SAL backend provides native S3-compatible multipart upload:
//!
//! ```no_run
//! # use object_store::rgw_sal::SalClient;
//! # use object_store::{ObjectStore, Path};
//! # async fn example() -> object_store::Result<()> {
//! # let client = SalClient::new("ceph".into(), "admin".into(), None, "test".into())?;
//! let path = Path::from("large-file.bin");
//! let mut upload = client.put_multipart(&path).await?;
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
//! The SAL backend uses a TRUE 1-1 mapping architecture:
//!
//! ```text
//! ┌─────────────────────────────────┐
//! │  ObjectStore Trait (Rust)       │
//! └────────────┬────────────────────┘
//!              │ Each method = 1 call
//!              ▼
//! ┌─────────────────────────────────┐
//! │  SalClient (client_v2.rs)       │
//! │  - Thin async wrapper           │
//! └────────────┬────────────────────┘
//!              │ 1 FFI call per method
//!              ▼
//! ┌─────────────────────────────────┐
//! │  Unified C API (Ceph repo)      │
//! │  - rgw_sal_unified.h/.cc        │
//! │  - ALL business logic here      │
//! └────────────┬────────────────────┘
//!              │ Internal C++ calls
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
//! - Ceph repository at `../ceph` (for building unified C API)
//! - Ceph development libraries (librados-dev, librgw-dev)
//! - C++17 compiler
//! - Build with `unified-sal` feature enabled
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

// Unified SAL implementation with TRUE 1-1 mapping
mod ffi_v2;
mod client_v2;

pub use client_v2::SalClient;

// Re-export commonly used types for convenience
pub use crate::{Error, ObjectStore, Path, Result};
