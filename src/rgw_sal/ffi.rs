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

//! FFI bindings to the C++ SAL wrapper
//!
//! This module provides low-level Foreign Function Interface (FFI) bindings
//! to the C++ RGW SAL (Storage Abstraction Layer) wrapper. All types and
//! functions here use C-compatible representations.

use std::os::raw::{c_char, c_int};

// ============================================================================
// Opaque C++ Types
// ============================================================================
// These are zero-sized opaque types representing C++ objects. We never access
// their internals, only pass pointers to them across the FFI boundary.

/// Opaque type for Ceph Context (CephContext)
#[repr(C)]
pub struct CephContext {
    _private: [u8; 0],
}

/// Opaque type for DoutPrefixProvider (logging context)
#[repr(C)]
pub struct DoutPrefixProvider {
    _private: [u8; 0],
}

/// Opaque type for SAL Driver
#[repr(C)]
pub struct SalDriver {
    _private: [u8; 0],
}

/// Opaque type for SAL User
#[repr(C)]
pub struct SalUser {
    _private: [u8; 0],
}

/// Opaque type for SAL Bucket
#[repr(C)]
pub struct SalBucket {
    _private: [u8; 0],
}

/// Opaque type for SAL Object
#[repr(C)]
pub struct SalObject {
    _private: [u8; 0],
}

/// Opaque type for SAL Writer
#[repr(C)]
pub struct SalWriter {
    _private: [u8; 0],
}

/// Opaque type for SAL MultipartUpload
#[repr(C)]
pub struct SalMultipartUpload {
    _private: [u8; 0],
}

/// Opaque type for SAL ReadOp
#[repr(C)]
pub struct SalReadOp {
    _private: [u8; 0],
}

// ============================================================================
// C-Compatible Structs
// ============================================================================

/// Object metadata (C-compatible)
#[repr(C)]
pub struct SalObjectMeta {
    pub size: u64,
    pub mtime: i64,  // Unix timestamp
    pub etag: *const c_char,
}

/// List entry for object listing
#[repr(C)]
pub struct SalListEntry {
    pub key: *const c_char,
    pub size: u64,
    pub mtime: i64,
}

// ============================================================================
// FFI Function Declarations
// ============================================================================

#[link(name = "rgw_sal_wrapper", kind = "static")]
extern "C" {
    // ========================================================================
    // Initialization and Cleanup
    // ========================================================================

    /// Create a Ceph context
    ///
    /// # Arguments
    /// * `cluster_name` - Name of the Ceph cluster (e.g., "ceph")
    /// * `user_name` - User name (e.g., "admin")
    /// * `conf_file` - Path to ceph.conf file (can be NULL)
    ///
    /// # Returns
    /// * Pointer to CephContext or NULL on error
    pub fn sal_create_ceph_context(
        cluster_name: *const c_char,
        user_name: *const c_char,
        conf_file: *const c_char,
    ) -> *mut CephContext;

    /// Destroy a Ceph context
    pub fn sal_destroy_ceph_context(cct: *mut CephContext);

    /// Create a RADOS-backed SAL driver
    ///
    /// # Arguments
    /// * `cct` - Ceph context
    /// * `dpp` - DoutPrefixProvider for logging (can be NULL)
    ///
    /// # Returns
    /// * Pointer to SalDriver or NULL on error
    pub fn sal_create_rados_driver(
        cct: *mut CephContext,
        dpp: *mut DoutPrefixProvider,
    ) -> *mut SalDriver;

    /// Destroy a SAL driver
    pub fn sal_destroy_driver(driver: *mut SalDriver);

    // ========================================================================
    // Bucket Operations
    // ========================================================================

    /// Get a bucket handle
    ///
    /// # Arguments
    /// * `driver` - SAL driver
    /// * `dpp` - DoutPrefixProvider for logging (can be NULL)
    /// * `bucket_name` - Name of the bucket
    /// * `tenant` - Tenant name (can be NULL for default tenant)
    ///
    /// # Returns
    /// * Pointer to SalBucket or NULL if bucket not found
    pub fn sal_get_bucket(
        driver: *mut SalDriver,
        dpp: *mut DoutPrefixProvider,
        bucket_name: *const c_char,
        tenant: *const c_char,
    ) -> *mut SalBucket;

    /// Destroy a bucket handle
    pub fn sal_destroy_bucket(bucket: *mut SalBucket);

    /// List objects in a bucket
    ///
    /// # Arguments
    /// * `bucket` - Bucket handle
    /// * `dpp` - DoutPrefixProvider for logging
    /// * `prefix` - Object key prefix filter (can be NULL for all objects)
    /// * `delimiter` - Delimiter for hierarchical listing (can be NULL)
    /// * `max_keys` - Maximum number of keys to return
    /// * `marker` - Continuation marker (can be NULL for first page)
    /// * `out_keys` - Output array of key names (caller must free)
    /// * `out_count` - Number of keys returned
    ///
    /// # Returns
    /// * 0 on success, negative error code on failure
    pub fn sal_list_objects(
        bucket: *mut SalBucket,
        dpp: *mut DoutPrefixProvider,
        prefix: *const c_char,
        delimiter: *const c_char,
        max_keys: c_int,
        marker: *const c_char,
        out_keys: *mut *mut *mut c_char,
        out_count: *mut c_int,
    ) -> c_int;

    /// Free the array returned by sal_list_objects
    pub fn sal_free_list_result(keys: *mut *mut c_char, count: c_int);

    // ========================================================================
    // Object Operations
    // ========================================================================

    /// Get an object handle
    ///
    /// # Arguments
    /// * `bucket` - Bucket handle
    /// * `dpp` - DoutPrefixProvider for logging
    /// * `key` - Object key name
    ///
    /// # Returns
    /// * Pointer to SalObject or NULL on error
    pub fn sal_get_object(
        bucket: *mut SalBucket,
        dpp: *mut DoutPrefixProvider,
        key: *const c_char,
    ) -> *mut SalObject;

    /// Destroy an object handle
    pub fn sal_destroy_object(object: *mut SalObject);

    /// Get object metadata
    ///
    /// # Arguments
    /// * `object` - Object handle
    /// * `dpp` - DoutPrefixProvider for logging
    /// * `out_meta` - Output metadata structure
    ///
    /// # Returns
    /// * 0 on success, negative error code on failure (e.g., -ENOENT if not found)
    pub fn sal_get_object_meta(
        object: *mut SalObject,
        dpp: *mut DoutPrefixProvider,
        out_meta: *mut SalObjectMeta,
    ) -> c_int;

    /// Read object data
    ///
    /// # Arguments
    /// * `object` - Object handle
    /// * `dpp` - DoutPrefixProvider for logging
    /// * `offset` - Byte offset to start reading from
    /// * `length` - Number of bytes to read
    /// * `out_buffer` - Output buffer (allocated by function, caller must free)
    /// * `out_bytes_read` - Actual number of bytes read
    ///
    /// # Returns
    /// * 0 on success, negative error code on failure
    pub fn sal_read_object(
        object: *mut SalObject,
        dpp: *mut DoutPrefixProvider,
        offset: u64,
        length: u64,
        out_buffer: *mut *mut c_char,
        out_bytes_read: *mut u64,
    ) -> c_int;

    /// Free buffer allocated by sal_read_object
    pub fn sal_free_read_buffer(buffer: *mut c_char);

    /// Delete an object
    ///
    /// # Arguments
    /// * `object` - Object handle
    /// * `dpp` - DoutPrefixProvider for logging
    ///
    /// # Returns
    /// * 0 on success, negative error code on failure
    pub fn sal_delete_object(
        object: *mut SalObject,
        dpp: *mut DoutPrefixProvider,
    ) -> c_int;

    // ========================================================================
    // Writer Operations (for PUT)
    // ========================================================================

    /// Create a writer for atomic write operations
    ///
    /// # Arguments
    /// * `driver` - SAL driver
    /// * `dpp` - DoutPrefixProvider for logging
    /// * `bucket` - Bucket handle
    /// * `key` - Object key name
    ///
    /// # Returns
    /// * Pointer to SalWriter or NULL on error
    pub fn sal_create_writer(
        driver: *mut SalDriver,
        dpp: *mut DoutPrefixProvider,
        bucket: *mut SalBucket,
        key: *const c_char,
    ) -> *mut SalWriter;

    /// Destroy a writer handle
    pub fn sal_destroy_writer(writer: *mut SalWriter);

    /// Prepare the writer for writing
    ///
    /// # Arguments
    /// * `writer` - Writer handle
    /// * `dpp` - DoutPrefixProvider for logging
    ///
    /// # Returns
    /// * 0 on success, negative error code on failure
    pub fn sal_writer_prepare(
        writer: *mut SalWriter,
        dpp: *mut DoutPrefixProvider,
    ) -> c_int;

    /// Write data using the writer
    ///
    /// # Arguments
    /// * `writer` - Writer handle
    /// * `dpp` - DoutPrefixProvider for logging
    /// * `data` - Data buffer to write
    /// * `length` - Length of data
    /// * `offset` - Offset within the object
    ///
    /// # Returns
    /// * 0 on success, negative error code on failure
    pub fn sal_writer_write(
        writer: *mut SalWriter,
        dpp: *mut DoutPrefixProvider,
        data: *const c_char,
        length: u64,
        offset: u64,
    ) -> c_int;

    /// Complete the write operation atomically
    ///
    /// # Arguments
    /// * `writer` - Writer handle
    /// * `dpp` - DoutPrefixProvider for logging
    /// * `out_etag` - Output ETag (allocated by function, caller must free)
    ///
    /// # Returns
    /// * 0 on success, negative error code on failure
    pub fn sal_writer_complete(
        writer: *mut SalWriter,
        dpp: *mut DoutPrefixProvider,
        out_etag: *mut *mut c_char,
    ) -> c_int;

    /// Free ETag string allocated by sal_writer_complete
    pub fn sal_free_etag(etag: *mut c_char);

    // ========================================================================
    // Multipart Upload Operations
    // ========================================================================

    /// Initiate a multipart upload
    ///
    /// # Arguments
    /// * `driver` - SAL driver
    /// * `dpp` - DoutPrefixProvider for logging
    /// * `bucket` - Bucket handle
    /// * `key` - Object key name
    /// * `out_upload_id` - Output upload ID (allocated by function, caller must free)
    ///
    /// # Returns
    /// * Pointer to SalMultipartUpload or NULL on error
    pub fn sal_init_multipart(
        driver: *mut SalDriver,
        dpp: *mut DoutPrefixProvider,
        bucket: *mut SalBucket,
        key: *const c_char,
        out_upload_id: *mut *mut c_char,
    ) -> *mut SalMultipartUpload;

    /// Destroy a multipart upload handle
    pub fn sal_destroy_multipart(upload: *mut SalMultipartUpload);

    /// Upload a part in a multipart upload
    ///
    /// # Arguments
    /// * `upload` - Multipart upload handle
    /// * `dpp` - DoutPrefixProvider for logging
    /// * `part_num` - Part number (1-based)
    /// * `data` - Part data buffer
    /// * `length` - Length of part data
    /// * `out_etag` - Output part ETag (allocated by function, caller must free)
    ///
    /// # Returns
    /// * 0 on success, negative error code on failure
    pub fn sal_upload_part(
        upload: *mut SalMultipartUpload,
        dpp: *mut DoutPrefixProvider,
        part_num: c_int,
        data: *const c_char,
        length: u64,
        out_etag: *mut *mut c_char,
    ) -> c_int;

    /// Complete a multipart upload
    ///
    /// # Arguments
    /// * `upload` - Multipart upload handle
    /// * `dpp` - DoutPrefixProvider for logging
    /// * `part_etags` - Array of part ETags (in order)
    /// * `num_parts` - Number of parts
    /// * `out_etag` - Output final ETag (allocated by function, caller must free)
    ///
    /// # Returns
    /// * 0 on success, negative error code on failure
    pub fn sal_complete_multipart(
        upload: *mut SalMultipartUpload,
        dpp: *mut DoutPrefixProvider,
        part_etags: *const *const c_char,
        num_parts: c_int,
        out_etag: *mut *mut c_char,
    ) -> c_int;

    /// Abort a multipart upload
    ///
    /// # Arguments
    /// * `upload` - Multipart upload handle
    /// * `dpp` - DoutPrefixProvider for logging
    ///
    /// # Returns
    /// * 0 on success, negative error code on failure
    pub fn sal_abort_multipart(
        upload: *mut SalMultipartUpload,
        dpp: *mut DoutPrefixProvider,
    ) -> c_int;

    /// Free upload ID string
    pub fn sal_free_upload_id(upload_id: *mut c_char);

    // ========================================================================
    // Copy Operations
    // ========================================================================

    /// Copy an object (server-side)
    ///
    /// # Arguments
    /// * `driver` - SAL driver
    /// * `dpp` - DoutPrefixProvider for logging
    /// * `src_bucket` - Source bucket handle
    /// * `src_key` - Source object key
    /// * `dst_bucket` - Destination bucket handle
    /// * `dst_key` - Destination object key
    ///
    /// # Returns
    /// * 0 on success, negative error code on failure
    pub fn sal_copy_object(
        driver: *mut SalDriver,
        dpp: *mut DoutPrefixProvider,
        src_bucket: *mut SalBucket,
        src_key: *const c_char,
        dst_bucket: *mut SalBucket,
        dst_key: *const c_char,
    ) -> c_int;
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Map SAL error codes to object_store errors
pub(crate) fn map_sal_error(code: c_int, context: &str) -> crate::Error {
    use crate::Error;

    match code {
        -2 => Error::NotFound {  // ENOENT
            path: context.to_string(),
            source: Box::new(std::io::Error::from_raw_os_error(-code)),
        },
        -17 => Error::AlreadyExists {  // EEXIST
            path: context.to_string(),
            source: Box::new(std::io::Error::from_raw_os_error(-code)),
        },
        -13 => Error::Generic {  // EACCES
            store: "SAL",
            source: Box::new(std::io::Error::from_raw_os_error(-code)),
        },
        -22 => Error::Generic {  // EINVAL
            store: "SAL",
            source: format!("Invalid argument: {}", context).into(),
        },
        _ => Error::Generic {
            store: "SAL",
            source: format!("{} failed with error code: {}", context, code).into(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_mapping() {
        let err = map_sal_error(-2, "test/path");
        assert!(matches!(err, crate::Error::NotFound { .. }));

        let err = map_sal_error(-17, "test/path");
        assert!(matches!(err, crate::Error::AlreadyExists { .. }));

        let err = map_sal_error(-13, "test/path");
        assert!(matches!(err, crate::Error::Generic { .. }));
    }
}
