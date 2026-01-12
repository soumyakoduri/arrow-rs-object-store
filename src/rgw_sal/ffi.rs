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

//! FFI bindings to RGW SAL (Storage Abstraction Layer)
//!
//! This module provides Rust bindings to the Ceph RGW SAL C++ API.
//! SAL is RGW's internal abstraction layer for different storage backends.

use std::os::raw::{c_char, c_int, c_void};

/// Opaque handle to SAL Driver (main entry point to SAL)
#[repr(C)]
pub struct SalDriver {
    _private: [u8; 0],
}

/// Opaque handle to SAL User
#[repr(C)]
pub struct SalUser {
    _private: [u8; 0],
}

/// Opaque handle to SAL Bucket
#[repr(C)]
pub struct SalBucket {
    _private: [u8; 0],
}

/// Opaque handle to SAL Object
#[repr(C)]
pub struct SalObject {
    _private: [u8; 0],
}

/// Opaque handle to SAL Writer (for PUT operations)
#[repr(C)]
pub struct SalWriter {
    _private: [u8; 0],
}

/// Opaque handle to SAL MultipartUpload
#[repr(C)]
pub struct SalMultipartUpload {
    _private: [u8; 0],
}

/// Opaque handle to Ceph context
#[repr(C)]
pub struct CephContext {
    _private: [u8; 0],
}

/// Opaque handle to DPP (Debug Print Provider)
#[repr(C)]
pub struct DoutPrefixProvider {
    _private: [u8; 0],
}

/// Object metadata from SAL
#[repr(C)]
pub struct SalObjectMeta {
    pub size: u64,
    pub mtime: i64,
    pub etag: *const c_char,
}

/// Result of a SAL operation
#[repr(C)]
pub struct SalResult {
    pub ret_code: c_int,
    pub error_message: *const c_char,
}

// Link to our C++ wrapper library
#[link(name = "rgw_sal_wrapper", kind = "static")]
extern "C" {
    // === Initialization ===

    /// Create a CephContext for SAL operations
    ///
    /// # Arguments
    /// * `cluster_name` - Ceph cluster name (e.g., "ceph")
    /// * `user_name` - User name (e.g., "admin")
    /// * `conf_file` - Path to ceph.conf (can be null for default)
    ///
    /// # Returns
    /// CephContext pointer on success, null on failure
    pub fn sal_create_ceph_context(
        cluster_name: *const c_char,
        user_name: *const c_char,
        conf_file: *const c_char,
    ) -> *mut CephContext;

    /// Configure authentication for CephContext
    ///
    /// # Arguments
    /// * `cct` - CephContext
    /// * `keyring` - Path to keyring file (can be null)
    /// * `key` - Key string (can be null)
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn sal_configure_auth(
        cct: *mut CephContext,
        keyring: *const c_char,
        key: *const c_char,
    ) -> c_int;

    /// Create a SAL Driver instance (RADOSStore backend)
    ///
    /// # Arguments
    /// * `cct` - CephContext
    /// * `dpp` - Debug print provider (can be null)
    ///
    /// # Returns
    /// SAL Driver pointer on success, null on failure
    pub fn sal_create_rados_driver(
        cct: *mut CephContext,
        dpp: *mut DoutPrefixProvider,
    ) -> *mut SalDriver;

    /// Destroy SAL Driver
    ///
    /// # Arguments
    /// * `driver` - SAL Driver to destroy
    pub fn sal_destroy_driver(driver: *mut SalDriver);

    /// Destroy CephContext
    ///
    /// # Arguments
    /// * `cct` - CephContext to destroy
    pub fn sal_destroy_ceph_context(cct: *mut CephContext);

    // === Bucket Operations ===

    /// Get a bucket by name
    ///
    /// # Arguments
    /// * `driver` - SAL Driver
    /// * `dpp` - Debug print provider (can be null)
    /// * `bucket_name` - Name of the bucket
    /// * `tenant` - Tenant name (can be null for default)
    ///
    /// # Returns
    /// SAL Bucket pointer on success, null on failure
    pub fn sal_get_bucket(
        driver: *mut SalDriver,
        dpp: *mut DoutPrefixProvider,
        bucket_name: *const c_char,
        tenant: *const c_char,
    ) -> *mut SalBucket;

    /// Destroy SAL Bucket
    ///
    /// # Arguments
    /// * `bucket` - SAL Bucket to destroy
    pub fn sal_destroy_bucket(bucket: *mut SalBucket);

    /// List objects in a bucket
    ///
    /// # Arguments
    /// * `bucket` - SAL Bucket
    /// * `dpp` - Debug print provider (can be null)
    /// * `prefix` - Object prefix filter (can be null)
    /// * `delimiter` - Delimiter for grouping (can be null)
    /// * `max_keys` - Maximum number of keys to return
    /// * `marker` - Continuation marker (can be null)
    /// * `out_keys` - Output buffer for object keys
    /// * `out_count` - Output count of returned keys
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
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

    /// Free object list returned by sal_list_objects
    ///
    /// # Arguments
    /// * `keys` - Array of keys to free
    /// * `count` - Number of keys
    pub fn sal_free_object_list(keys: *mut *mut c_char, count: c_int);

    // === Object Operations ===

    /// Get an object from a bucket
    ///
    /// # Arguments
    /// * `bucket` - SAL Bucket
    /// * `dpp` - Debug print provider (can be null)
    /// * `key` - Object key
    ///
    /// # Returns
    /// SAL Object pointer on success, null on failure
    pub fn sal_get_object(
        bucket: *mut SalBucket,
        dpp: *mut DoutPrefixProvider,
        key: *const c_char,
    ) -> *mut SalObject;

    /// Destroy SAL Object
    ///
    /// # Arguments
    /// * `object` - SAL Object to destroy
    pub fn sal_destroy_object(object: *mut SalObject);

    /// Get object metadata
    ///
    /// # Arguments
    /// * `object` - SAL Object
    /// * `dpp` - Debug print provider (can be null)
    /// * `out_meta` - Output metadata structure
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn sal_get_object_meta(
        object: *mut SalObject,
        dpp: *mut DoutPrefixProvider,
        out_meta: *mut SalObjectMeta,
    ) -> c_int;

    /// Read object data
    ///
    /// # Arguments
    /// * `object` - SAL Object
    /// * `dpp` - Debug print provider (can be null)
    /// * `offset` - Offset to read from
    /// * `length` - Number of bytes to read
    /// * `out_buffer` - Output buffer (allocated by function)
    /// * `out_bytes_read` - Actual bytes read
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn sal_read_object(
        object: *mut SalObject,
        dpp: *mut DoutPrefixProvider,
        offset: u64,
        length: u64,
        out_buffer: *mut *mut c_char,
        out_bytes_read: *mut u64,
    ) -> c_int;

    /// Free buffer allocated by sal_read_object
    ///
    /// # Arguments
    /// * `buffer` - Buffer to free
    pub fn sal_free_read_buffer(buffer: *mut c_char);

    /// Delete an object
    ///
    /// # Arguments
    /// * `object` - SAL Object
    /// * `dpp` - Debug print provider (can be null)
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn sal_delete_object(
        object: *mut SalObject,
        dpp: *mut DoutPrefixProvider,
    ) -> c_int;

    // === Writer Operations (PUT) ===

    /// Create a writer for putting an object
    ///
    /// # Arguments
    /// * `driver` - SAL Driver
    /// * `dpp` - Debug print provider (can be null)
    /// * `bucket` - SAL Bucket
    /// * `key` - Object key
    ///
    /// # Returns
    /// SAL Writer pointer on success, null on failure
    pub fn sal_create_writer(
        driver: *mut SalDriver,
        dpp: *mut DoutPrefixProvider,
        bucket: *mut SalBucket,
        key: *const c_char,
    ) -> *mut SalWriter;

    /// Destroy SAL Writer
    ///
    /// # Arguments
    /// * `writer` - SAL Writer to destroy
    pub fn sal_destroy_writer(writer: *mut SalWriter);

    /// Prepare writer for writing
    ///
    /// # Arguments
    /// * `writer` - SAL Writer
    /// * `dpp` - Debug print provider (can be null)
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn sal_writer_prepare(
        writer: *mut SalWriter,
        dpp: *mut DoutPrefixProvider,
    ) -> c_int;

    /// Write data using the writer
    ///
    /// # Arguments
    /// * `writer` - SAL Writer
    /// * `dpp` - Debug print provider (can be null)
    /// * `data` - Data buffer
    /// * `length` - Length of data
    /// * `offset` - Write offset
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn sal_writer_write(
        writer: *mut SalWriter,
        dpp: *mut DoutPrefixProvider,
        data: *const c_char,
        length: u64,
        offset: u64,
    ) -> c_int;

    /// Complete the write operation
    ///
    /// # Arguments
    /// * `writer` - SAL Writer
    /// * `dpp` - Debug print provider (can be null)
    /// * `out_etag` - Output ETag (allocated by function)
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn sal_writer_complete(
        writer: *mut SalWriter,
        dpp: *mut DoutPrefixProvider,
        out_etag: *mut *mut c_char,
    ) -> c_int;

    /// Free ETag string allocated by sal_writer_complete
    ///
    /// # Arguments
    /// * `etag` - ETag to free
    pub fn sal_free_etag(etag: *mut c_char);

    // === Multipart Upload Operations ===

    /// Initialize a multipart upload
    ///
    /// # Arguments
    /// * `writer` - SAL Writer
    /// * `dpp` - Debug print provider (can be null)
    /// * `upload_id` - Upload ID
    ///
    /// # Returns
    /// SAL MultipartUpload pointer on success, null on failure
    pub fn sal_init_multipart(
        writer: *mut SalWriter,
        dpp: *mut DoutPrefixProvider,
        upload_id: *const c_char,
    ) -> *mut SalMultipartUpload;

    /// Destroy SAL MultipartUpload
    ///
    /// # Arguments
    /// * `upload` - SAL MultipartUpload to destroy
    pub fn sal_destroy_multipart(upload: *mut SalMultipartUpload);

    /// Upload a part
    ///
    /// # Arguments
    /// * `upload` - SAL MultipartUpload
    /// * `dpp` - Debug print provider (can be null)
    /// * `part_num` - Part number (starting from 1)
    /// * `data` - Part data
    /// * `length` - Length of data
    /// * `out_etag` - Output part ETag (allocated by function)
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
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
    /// * `upload` - SAL MultipartUpload
    /// * `dpp` - Debug print provider (can be null)
    /// * `part_etags` - Array of part ETags
    /// * `num_parts` - Number of parts
    /// * `out_final_etag` - Output final ETag (allocated by function)
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn sal_complete_multipart(
        upload: *mut SalMultipartUpload,
        dpp: *mut DoutPrefixProvider,
        part_etags: *const *const c_char,
        num_parts: c_int,
        out_final_etag: *mut *mut c_char,
    ) -> c_int;

    /// Abort a multipart upload
    ///
    /// # Arguments
    /// * `upload` - SAL MultipartUpload
    /// * `dpp` - Debug print provider (can be null)
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn sal_abort_multipart(
        upload: *mut SalMultipartUpload,
        dpp: *mut DoutPrefixProvider,
    ) -> c_int;
}

/// Error codes (from errno.h)
#[allow(dead_code)]
pub mod errors {
    use std::os::raw::c_int;

    /// Object not found
    pub const ENOENT: c_int = -2;

    /// Permission denied
    pub const EACCES: c_int = -13;

    /// Object already exists
    pub const EEXIST: c_int = -17;

    /// Invalid argument
    pub const EINVAL: c_int = -22;

    /// No space left
    pub const ENOSPC: c_int = -28;

    /// Operation timed out
    pub const ETIMEDOUT: c_int = -110;
}
