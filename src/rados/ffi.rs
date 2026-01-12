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

//! Raw FFI bindings to librados
//!
//! This module provides minimal, focused FFI bindings to the Ceph librados library.
//! We only bind the specific functions needed for the object store implementation.

use std::os::raw::{c_char, c_int, c_void};

/// Opaque handle to a RADOS cluster connection
#[repr(C)]
pub struct RadosT {
    _private: [u8; 0],
}

/// Opaque handle to a RADOS I/O context
#[repr(C)]
pub struct RadosIoCtxT {
    _private: [u8; 0],
}

/// Opaque handle to a RADOS list context
#[repr(C)]
pub struct RadosListCtxT {
    _private: [u8; 0],
}

/// Opaque handle to a RADOS completion (for async operations)
#[repr(C)]
pub struct RadosCompletionT {
    _private: [u8; 0],
}

// Link to librados
#[link(name = "rados")]
extern "C" {
    // === Cluster Operations ===

    /// Create a cluster handle
    ///
    /// # Arguments
    /// * `cluster` - where to store the handle
    /// * `id` - user ID (e.g., "admin" for "client.admin")
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn rados_create(cluster: *mut *mut RadosT, id: *const c_char) -> c_int;

    /// Read a Ceph configuration file
    ///
    /// # Arguments
    /// * `cluster` - cluster handle
    /// * `path` - path to ceph.conf file
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn rados_conf_read_file(cluster: *mut RadosT, path: *const c_char) -> c_int;

    /// Set a configuration option
    ///
    /// # Arguments
    /// * `cluster` - cluster handle
    /// * `option` - option name (e.g., "keyring", "key")
    /// * `value` - option value
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn rados_conf_set(
        cluster: *mut RadosT,
        option: *const c_char,
        value: *const c_char,
    ) -> c_int;

    /// Connect to the Ceph cluster
    ///
    /// # Arguments
    /// * `cluster` - cluster handle
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn rados_connect(cluster: *mut RadosT) -> c_int;

    /// Shutdown and free cluster handle
    ///
    /// # Arguments
    /// * `cluster` - cluster handle to shutdown
    pub fn rados_shutdown(cluster: *mut RadosT);

    // === I/O Context Operations ===

    /// Create an I/O context
    ///
    /// # Arguments
    /// * `cluster` - cluster handle
    /// * `pool_name` - name of the pool
    /// * `ioctx` - where to store the I/O context
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn rados_ioctx_create(
        cluster: *mut RadosT,
        pool_name: *const c_char,
        ioctx: *mut *mut RadosIoCtxT,
    ) -> c_int;

    /// Set the namespace for an I/O context
    ///
    /// # Arguments
    /// * `io` - I/O context
    /// * `nspace` - namespace name
    pub fn rados_ioctx_set_namespace(io: *mut RadosIoCtxT, nspace: *const c_char);

    /// Destroy an I/O context
    ///
    /// # Arguments
    /// * `io` - I/O context to destroy
    pub fn rados_ioctx_destroy(io: *mut RadosIoCtxT);

    // === Object Operations ===

    /// Write an entire object
    ///
    /// # Arguments
    /// * `io` - I/O context
    /// * `oid` - object ID/name
    /// * `buf` - data to write
    /// * `len` - length of data
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn rados_write_full(
        io: *mut RadosIoCtxT,
        oid: *const c_char,
        buf: *const c_char,
        len: usize,
    ) -> c_int;

    /// Read from an object
    ///
    /// # Arguments
    /// * `io` - I/O context
    /// * `oid` - object ID/name
    /// * `buf` - buffer to read into
    /// * `len` - number of bytes to read
    /// * `off` - offset to start reading from
    ///
    /// # Returns
    /// Number of bytes read on success, negative error code on failure
    pub fn rados_read(
        io: *mut RadosIoCtxT,
        oid: *const c_char,
        buf: *mut c_char,
        len: usize,
        off: u64,
    ) -> c_int;

    /// Get object size and modification time
    ///
    /// # Arguments
    /// * `io` - I/O context
    /// * `oid` - object ID/name
    /// * `psize` - where to store size (can be null)
    /// * `pmtime` - where to store mtime (can be null)
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn rados_stat(
        io: *mut RadosIoCtxT,
        oid: *const c_char,
        psize: *mut u64,
        pmtime: *mut i64,
    ) -> c_int;

    /// Remove an object
    ///
    /// # Arguments
    /// * `io` - I/O context
    /// * `oid` - object ID/name
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn rados_remove(io: *mut RadosIoCtxT, oid: *const c_char) -> c_int;

    /// Append data to an object
    ///
    /// # Arguments
    /// * `io` - I/O context
    /// * `oid` - object ID/name
    /// * `buf` - data to append
    /// * `len` - length of data
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn rados_append(
        io: *mut RadosIoCtxT,
        oid: *const c_char,
        buf: *const c_char,
        len: usize,
    ) -> c_int;

    // === Object Listing ===

    /// Start listing objects in a pool
    ///
    /// # Arguments
    /// * `io` - I/O context
    /// * `ctx` - where to store the list context
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn rados_nobjects_list_open(
        io: *mut RadosIoCtxT,
        ctx: *mut *mut RadosListCtxT,
    ) -> c_int;

    /// Get the next object in a listing
    ///
    /// # Arguments
    /// * `ctx` - list context
    /// * `entry` - where to store the object name
    /// * `key` - where to store the key (can be null)
    /// * `nspace` - where to store the namespace (can be null)
    ///
    /// # Returns
    /// 0 on success, negative error code on completion or failure
    pub fn rados_nobjects_list_next(
        ctx: *mut RadosListCtxT,
        entry: *mut *const c_char,
        key: *mut *const c_char,
        nspace: *mut *const c_char,
    ) -> c_int;

    /// Close a list context
    ///
    /// # Arguments
    /// * `ctx` - list context to close
    pub fn rados_nobjects_list_close(ctx: *mut RadosListCtxT);

    // === Extended Attributes (for metadata) ===

    /// Get an extended attribute
    ///
    /// # Arguments
    /// * `io` - I/O context
    /// * `o` - object name
    /// * `name` - attribute name
    /// * `buf` - buffer to read into
    /// * `len` - buffer length
    ///
    /// # Returns
    /// Length of attribute value on success, negative error code on failure
    pub fn rados_getxattr(
        io: *mut RadosIoCtxT,
        o: *const c_char,
        name: *const c_char,
        buf: *mut c_char,
        len: usize,
    ) -> c_int;

    /// Set an extended attribute
    ///
    /// # Arguments
    /// * `io` - I/O context
    /// * `o` - object name
    /// * `name` - attribute name
    /// * `buf` - attribute value
    /// * `len` - value length
    ///
    /// # Returns
    /// 0 on success, negative error code on failure
    pub fn rados_setxattr(
        io: *mut RadosIoCtxT,
        o: *const c_char,
        name: *const c_char,
        buf: *const c_char,
        len: usize,
    ) -> c_int;
}

/// Error codes from librados
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_struct_sizes() {
        // These are opaque types, so we just verify they compile
        assert_eq!(std::mem::size_of::<*mut RadosT>(), std::mem::size_of::<usize>());
        assert_eq!(std::mem::size_of::<*mut RadosIoCtxT>(), std::mem::size_of::<usize>());
    }
}
