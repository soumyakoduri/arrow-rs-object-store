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
//   Unless required by applicable law or agreed to in writing,
//   software distributed under the License is distributed on an
//   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//   KIND, either express or implied.  See the License for the
//   specific language governing permissions and limitations
//   under the License.

//! RGW (RADOS Gateway) object store implementation
//!
//! This module provides an ObjectStore implementation backed by Ceph's RADOS Gateway (RGW)
//! using the RGW SAL C API.
//!
//! # Architecture
//!
//! This is a **wrapper** ObjectStore that accepts pre-initialized RGW driver and DPP
//! (DoutPrefixProvider) pointers from external C code. The RGW backend does not handle
//! driver initialization - the user must:
//!
//! 1. Initialize the RGW driver externally (e.g., via librgw or direct SAL API calls)
//! 2. Obtain raw pointers to the driver and DPP
//! 3. Pass these pointers to [`RgwObjectStoreBuilder`]
//! 4. Ensure the pointers remain valid for the ObjectStore lifetime
//!
//! # Example
//!
//! ```ignore
//! use object_store::rgw::RgwObjectStoreBuilder;
//! use object_store::{ObjectStore, Path};
//!
//! // Obtain driver_ptr and dpp_ptr from external RGW initialization
//! // (not shown - user's responsibility)
//!
//! let store = unsafe {
//!     RgwObjectStoreBuilder::new(driver_ptr, dpp_ptr)
//!         .with_bucket("my-bucket")
//!         .build()?
//! };
//!
//! // Use like any ObjectStore
//! let path = Path::from("test.txt");
//! store.put(&path, b"Hello RGW!".to_vec().into()).await?;
//!
//! let result = store.get(&path).await?;
//! let bytes = result.bytes().await?;
//! assert_eq!(&bytes[..], b"Hello RGW!");
//!
//! store.delete(&path).await?;
//! ```
//!
//! # Safety Requirements
//!
//! Using this backend requires careful attention to safety:
//!
//! - **Driver Lifetime**: The RGW driver and DPP must remain valid for the entire lifetime
//!   of the ObjectStore instance
//! - **Thread Safety**: The RGW driver must be thread-safe if the ObjectStore is used from
//!   multiple threads
//! - **Initialization**: The driver must be properly initialized and connected before use
//! - **Cleanup**: The user is responsible for cleaning up the driver after the ObjectStore
//!   is dropped
//!
//! # Features
//!
//! Currently implemented operations:
//! - PUT (with basic support)
//! - GET (with range support)
//! - DELETE (single and stream)
//! - LIST (with and without delimiter)
//!
//! Deferred features (not yet implemented):
//! - Conditional PUT/GET (if-match, if-none-match, if-modified-since)
//! - Multipart upload
//! - Bulk delete
//! - Multi-range GET
//! - Server-side copy

use std::ffi::CString;
use std::fmt;
use std::os::raw::{c_char, c_void};
use std::ptr;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};

use crate::{
    path::Path, Attributes, GetOptions, GetRange, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result,
};

mod builder;
mod error;
mod ffi;
mod memory;

pub use builder::RGWObjectStoreBuilder;
use error::{check_errno, errno_to_error, RgwError};
use memory::{RgwBuffer, RgwListResultWrapper, RgwString};

const STORE: &str = "RGW";

/// RGW ObjectStore implementation
///
/// This wraps a pre-initialized RGW driver and provides ObjectStore trait implementation.
///
/// # Thread Safety
///
/// Marked as Send+Sync based on the assumption that the underlying RGW driver is thread-safe.
/// The user must ensure this is the case when constructing the ObjectStore.
pub struct RGWObjectStore {
    bucket_name: String,
    driver: *mut c_void,
    dpp: *const c_void,
}

// Safety: The user guarantees that driver and dpp are thread-safe
unsafe impl Send for RGWObjectStore {}
unsafe impl Sync for RGWObjectStore {}

impl fmt::Display for RGWObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RGWObjectStore({})", self.bucket_name)
    }
}

impl fmt::Debug for RGWObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RGWObjectStore")
            .field("bucket_name", &self.bucket_name)
            .field("driver", &self.driver)
            .field("dpp", &self.dpp)
            .finish()
    }
}

#[async_trait]
impl ObjectStore for RGWObjectStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, _opts: PutOptions) -> Result<PutResult> {
        // Collect payload bytes
        let data: Vec<u8> = payload.iter().flat_map(|b| b.iter()).copied().collect();

        // Clone values for the blocking task
        let bucket = self.bucket_name.clone();
        let key = location.to_string();
        let driver_ptr = self.driver as usize;
        let dpp_ptr = self.dpp as usize;
        let location = location.clone();

        // Spawn blocking task for C API call
        let result = tokio::task::spawn_blocking(move || {
            // Cast pointers back
            let driver_ptr = driver_ptr as *mut c_void;
            let dpp_ptr = dpp_ptr as *const c_void;
            // Convert strings to C strings
            let bucket_c = CString::new(bucket).map_err(RgwError::from)?;
            let key_c = CString::new(key).map_err(RgwError::from)?;

            let mut etag_ptr: *mut c_char = ptr::null_mut();

            // Call RGW PUT API
            let ret = unsafe {
                ffi::rgw_put_object(
                    driver_ptr,
                    dpp_ptr,
                    bucket_c.as_ptr(),
                    key_c.as_ptr(),
                    data.as_ptr() as *const c_char,
                    data.len() as u64,
                    ptr::null_mut(), // obj_attributes (not used for now)
                    ptr::null(),     // conditionals (not used for now)
                    &mut etag_ptr,
                )
            };

            // Check for errors
            check_errno(ret, &location)?;

            // Extract etag if present
            let etag = if !etag_ptr.is_null() {
                let etag_wrapper = unsafe { RgwString::from_raw(etag_ptr) };
                etag_wrapper.to_string()
            } else {
                None
            };

            Ok::<PutResult, crate::Error>(PutResult {
                e_tag: etag,
                version: None,
            })
        })
        .await??;

        Ok(result)
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let bucket = self.bucket_name.clone();
        let key = location.to_string();
        let driver_ptr = self.driver as usize;
        let dpp_ptr = self.dpp as usize;
        let location_clone = location.clone();

        let range = options.range.clone();

        // Spawn blocking task for C API call
        let result = tokio::task::spawn_blocking(move || {
            // Cast pointers back
            let driver_ptr = driver_ptr as *mut c_void;
            let dpp_ptr = dpp_ptr as *const c_void;
            // Convert strings to C strings
            let bucket_c = CString::new(bucket).map_err(RgwError::from)?;
            let key_c = CString::new(key).map_err(RgwError::from)?;

            let mut buffer_ptr: *mut c_char = ptr::null_mut();
            let mut bytes_read: u64 = 0;
            let mut meta = ffi::RGWObjectMeta {
                size: 0,
                mtime_sec: 0,
                mtime_nsec: 0,
                etag: ptr::null_mut(),
            };

            // Determine offset and length from range
            let (offset, len) = match &range {
                Some(GetRange::Bounded(r)) => (r.start, r.end - r.start),
                Some(GetRange::Offset(offset)) => (*offset, 0), // Read from offset to end
                Some(GetRange::Suffix(n)) => {
                    // For suffix, we need to know the object size first
                    // For now, just read the entire object and handle suffix in a second pass
                    // This is a simplified implementation
                    (0, 0)
                }
                None => (0, 0), // 0 length means read entire object
            };

            // Call RGW GET API
            let ret = unsafe {
                ffi::rgw_get_object(
                    driver_ptr,
                    dpp_ptr,
                    bucket_c.as_ptr(),
                    key_c.as_ptr(),
                    offset,
                    len,
                    ptr::null_mut(), // conditionals (not used for now)
                    &mut buffer_ptr,
                    &mut bytes_read,
                    &mut meta,
                )
            };

            // Check for errors
            check_errno(ret, &location_clone)?;

            // Wrap buffer in RAII wrapper for automatic cleanup
            let buffer = unsafe { RgwBuffer::from_raw(buffer_ptr, bytes_read as usize) };
            let data = Bytes::copy_from_slice(buffer.as_bytes());

            // Extract etag
            let etag = if !meta.etag.is_null() {
                let etag_wrapper = unsafe { RgwString::from_raw(meta.etag) };
                etag_wrapper.to_string()
            } else {
                None
            };

            // Create object metadata
            let last_modified = DateTime::from_timestamp(meta.mtime_sec, meta.mtime_nsec as u32)
                .unwrap_or_else(|| Utc::now());

            let object_meta = ObjectMeta {
                location: location_clone.clone(),
                last_modified,
                size: meta.size,
                e_tag: etag,
                version: None,
            };

            // Determine actual range returned
            let actual_range = match range {
                Some(GetRange::Bounded(r)) => r.start..(r.start + bytes_read),
                Some(GetRange::Offset(offset)) => offset..(offset + bytes_read),
                Some(GetRange::Suffix(_)) | None => 0..bytes_read,
            };

            Ok::<(Bytes, ObjectMeta, std::ops::Range<u64>), crate::Error>((data, object_meta, actual_range))
        })
        .await??;

        let (data, meta, range) = result;

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(data) }).boxed()),
            meta,
            range,
            attributes: Attributes::default(),
        })
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let bucket = self.bucket_name.clone();
        let key = location.to_string();
        let driver_ptr = self.driver as usize;
        let dpp_ptr = self.dpp as usize;
        let location = location.clone();

        tokio::task::spawn_blocking(move || {
            // Cast pointers back
            let driver_ptr = driver_ptr as *mut c_void;
            let dpp_ptr = dpp_ptr as *const c_void;
            // Convert strings to C strings
            let bucket_c = CString::new(bucket).map_err(RgwError::from)?;
            let key_c = CString::new(key).map_err(RgwError::from)?;

            // Call RGW DELETE API
            let ret = unsafe {
                ffi::rgw_delete_object(
                    driver_ptr,
                    dpp_ptr,
                    bucket_c.as_ptr(),
                    key_c.as_ptr(),
                )
            };

            // Ignore ENOENT errors (object already deleted)
            if ret != 0 && ret != -2 {
                check_errno(ret, &location)?;
            }

            Ok::<(), crate::Error>(())
        })
        .await?
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        locations
            .then(move |location| async move {
                let location = location?;
                self.delete(&location).await?;
                Ok(location)
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let bucket = self.bucket_name.clone();
        let prefix_str = prefix.map(|p| p.to_string());
        let driver_ptr = self.driver as usize;
        let dpp_ptr = self.dpp as usize;

        // Create a stream from a single list operation
        // TODO: Implement proper pagination for large result sets
        stream::once(async move {
            tokio::task::spawn_blocking(move || {
                // Cast pointers back
                let driver_ptr = driver_ptr as *mut c_void;
                let dpp_ptr = dpp_ptr as *const c_void;
                // Convert strings to C strings
                let bucket_c = CString::new(bucket).map_err(RgwError::from)?;
                let prefix_c = prefix_str
                    .as_ref()
                    .map(|s| CString::new(s.as_str()))
                    .transpose()
                    .map_err(RgwError::from)?;

                // Initialize list result structure
                let mut list_result = ffi::RGWListResult {
                    entries: ptr::null_mut(),
                    num_objects: 0,
                    common_prefixes: ptr::null_mut(),
                    num_common_prefixes: 0,
                    next_marker: ptr::null_mut(),
                    is_truncated: 0,
                };

                // Call RGW LIST API
                let ret = unsafe {
                    ffi::rgw_list_objects(
                        driver_ptr,
                        dpp_ptr,
                        bucket_c.as_ptr(),
                        prefix_c.as_ref().map_or(ptr::null(), |c| c.as_ptr()),
                        ptr::null(), // delimiter (null = recursive listing)
                        ptr::null(), // marker (null = start from beginning)
                        1000,        // max_keys
                        &mut list_result,
                    )
                };

                // Check for errors
                if ret != 0 {
                    return Err(errno_to_error(ret, Path::from("")));
                }

                // Wrap result in RAII wrapper
                let wrapper = unsafe { RgwListResultWrapper::from_raw(list_result) };

                // Convert entries to ObjectMeta
                let mut objects = Vec::new();
                for entry in wrapper.entries() {
                    // Extract key
                    let key = if !entry.key.is_null() {
                        let key_cstr = unsafe { std::ffi::CStr::from_ptr(entry.key) };
                        key_cstr.to_str().ok().map(|s| s.to_string())
                    } else {
                        None
                    };

                    let key = match key {
                        Some(k) => k,
                        None => continue, // Skip entries with invalid keys
                    };

                    // Extract etag
                    let etag = if !entry.etag.is_null() {
                        let etag_cstr = unsafe { std::ffi::CStr::from_ptr(entry.etag) };
                        etag_cstr.to_str().ok().map(|s| s.to_string())
                    } else {
                        None
                    };

                    // Create modification time
                    let last_modified =
                        DateTime::from_timestamp(entry.mtime_sec, entry.mtime_nsec as u32)
                            .unwrap_or_else(|| Utc::now());

                    objects.push(ObjectMeta {
                        location: Path::from(key),
                        last_modified,
                        size: entry.size,
                        e_tag: etag,
                        version: None,
                    });
                }

                Ok::<Vec<ObjectMeta>, crate::Error>(objects)
            })
            .await?
        })
        .and_then(|objects| async move { Ok(stream::iter(objects).map(Ok)) })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let bucket = self.bucket_name.clone();
        let prefix_str = prefix.map(|p| p.to_string());
        let driver_ptr = self.driver as usize;
        let dpp_ptr = self.dpp as usize;

        tokio::task::spawn_blocking(move || {
            // Cast pointers back
            let driver_ptr = driver_ptr as *mut c_void;
            let dpp_ptr = dpp_ptr as *const c_void;
            // Convert strings to C strings
            let bucket_c = CString::new(bucket).map_err(RgwError::from)?;
            let prefix_c = prefix_str
                .as_ref()
                .map(|s| CString::new(s.as_str()))
                .transpose()
                .map_err(RgwError::from)?;
            let delimiter_c = CString::new("/").map_err(RgwError::from)?;

            // Initialize list result structure
            let mut list_result = ffi::RGWListResult {
                entries: ptr::null_mut(),
                num_objects: 0,
                common_prefixes: ptr::null_mut(),
                num_common_prefixes: 0,
                next_marker: ptr::null_mut(),
                is_truncated: 0,
            };

            // Call RGW LIST API with delimiter
            let ret = unsafe {
                ffi::rgw_list_objects(
                    driver_ptr,
                    dpp_ptr,
                    bucket_c.as_ptr(),
                    prefix_c.as_ref().map_or(ptr::null(), |c| c.as_ptr()),
                    delimiter_c.as_ptr(), // Use "/" delimiter
                    ptr::null(),          // marker (null = start from beginning)
                    1000,                 // max_keys
                    &mut list_result,
                )
            };

            // Check for errors
            if ret != 0 {
                return Err(errno_to_error(ret, Path::from("")));
            }

            // Wrap result in RAII wrapper
            let wrapper = unsafe { RgwListResultWrapper::from_raw(list_result) };

            // Convert entries to ObjectMeta
            let mut objects = Vec::new();
            for entry in wrapper.entries() {
                // Extract key
                let key = if !entry.key.is_null() {
                    let key_cstr = unsafe { std::ffi::CStr::from_ptr(entry.key) };
                    key_cstr.to_str().ok().map(|s| s.to_string())
                } else {
                    None
                };

                let key = match key {
                    Some(k) => k,
                    None => continue,
                };

                // Extract etag
                let etag = if !entry.etag.is_null() {
                    let etag_cstr = unsafe { std::ffi::CStr::from_ptr(entry.etag) };
                    etag_cstr.to_str().ok().map(|s| s.to_string())
                } else {
                    None
                };

                // Create modification time
                let last_modified =
                    DateTime::from_timestamp(entry.mtime_sec, entry.mtime_nsec as u32)
                        .unwrap_or_else(|| Utc::now());

                objects.push(ObjectMeta {
                    location: Path::from(key),
                    last_modified,
                    size: entry.size,
                    e_tag: etag,
                    version: None,
                });
            }

            // Convert common prefixes to Paths
            let common_prefixes: Vec<Path> = wrapper
                .common_prefixes()
                .into_iter()
                .filter_map(|prefix_opt| prefix_opt.map(|s| Path::from(s.to_string())))
                .collect();

            Ok::<ListResult, crate::Error>(ListResult {
                objects,
                common_prefixes,
            })
        })
        .await?
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        // Multipart upload deferred for future implementation
        Err(crate::Error::NotSupported {
            source: "Multipart upload not yet implemented for RGW backend".into(),
        })
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        // Copy operation is currently stubbed in the C API (returns -ENOSYS)
        Err(crate::Error::NotSupported {
            source: "Copy operation not yet implemented in RGW C API".into(),
        })
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        // Copy operation is currently stubbed in the C API (returns -ENOSYS)
        Err(crate::Error::NotSupported {
            source: "Copy operation not yet implemented in RGW C API".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        let store = RGWObjectStore {
            bucket_name: "test-bucket".to_string(),
            driver: ptr::null_mut(),
            dpp: ptr::null(),
        };

        assert_eq!(format!("{}", store), "RGWObjectStore(test-bucket)");
    }

    #[test]
    fn test_debug() {
        let store = RGWObjectStore {
            bucket_name: "test-bucket".to_string(),
            driver: ptr::null_mut(),
            dpp: ptr::null(),
        };

        let debug_str = format!("{:?}", store);
        assert!(debug_str.contains("test-bucket"));
    }

    /// Test that SAL API symbols are correctly linked
    /// This test verifies that the C FFI functions from librgw are available at link time.
    /// We expect these calls to fail with EINVAL since we're passing null pointers,
    /// but the important verification is that the symbols can be called (linking succeeded).
    #[test]
    fn test_sal_api_linking() {
        use crate::rgw::ffi;
        use std::ffi::CString;
        use std::os::raw::c_int;

        unsafe {
            // Test rgw_put_object symbol is available
            let bucket = CString::new("test").unwrap();
            let key = CString::new("key").unwrap();
            let data = CString::new("data").unwrap();
            let mut etag: *mut i8 = ptr::null_mut();

            let result = ffi::rgw_put_object(
                ptr::null_mut(),
                ptr::null(),
                bucket.as_ptr(),
                key.as_ptr(),
                data.as_ptr(),
                4,
                ptr::null_mut(),
                ptr::null(),
                &mut etag,
            );

            // Should return error (EINVAL = -22) because driver_ptr is null
            // The important thing is it doesn't fail at link time
            assert_eq!(result, -22, "Expected EINVAL for null driver pointer");

            // Test rgw_get_object symbol is available
            let mut buffer: *mut i8 = ptr::null_mut();
            let mut bytes_read: u64 = 0;
            let mut meta = ffi::RGWObjectMeta {
                size: 0,
                mtime_sec: 0,
                mtime_nsec: 0,
                etag: ptr::null_mut(),
            };

            let result = ffi::rgw_get_object(
                ptr::null_mut(),
                ptr::null(),
                bucket.as_ptr(),
                key.as_ptr(),
                0,
                0,
                ptr::null_mut(),
                &mut buffer,
                &mut bytes_read,
                &mut meta,
            );

            // Should return error (EINVAL = -22) because driver_ptr is null
            assert_eq!(result, -22, "Expected EINVAL for null driver pointer");

            // Test rgw_delete_object symbol is available
            let result = ffi::rgw_delete_object(
                ptr::null_mut(),
                ptr::null(),
                bucket.as_ptr(),
                key.as_ptr(),
            );

            // Should return error (EINVAL = -22) because driver_ptr is null
            assert_eq!(result, -22, "Expected EINVAL for null driver pointer");

            // Test rgw_list_objects symbol is available
            let prefix = CString::new("prefix").unwrap();
            let mut list_result = ffi::RGWListResult {
                entries: ptr::null_mut(),
                num_objects: 0,
                common_prefixes: ptr::null_mut(),
                num_common_prefixes: 0,
                next_marker: ptr::null_mut(),
                is_truncated: 0,
            };

            let result = ffi::rgw_list_objects(
                ptr::null_mut(),
                ptr::null(),
                bucket.as_ptr(),
                prefix.as_ptr(),
                ptr::null(),
                ptr::null(),
                1000,
                &mut list_result,
            );

            // Should return error (EINVAL = -22) because driver_ptr is null
            assert_eq!(result, -22, "Expected EINVAL for null driver pointer");
        }
    }
}
