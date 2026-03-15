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

//! RAII wrappers for C-allocated memory
//!
//! This module provides safe Rust wrappers around memory allocated by the RGW C API.
//! All wrappers implement Drop to ensure automatic cleanup when they go out of scope.

use std::ffi::CStr;
use std::os::raw::c_char;

use crate::rgw::ffi;

/// RAII wrapper for C-allocated data buffer
///
/// Automatically freed with `rgw_free_buffer` when dropped.
pub struct RgwBuffer(*mut c_char, usize);

impl RgwBuffer {
    /// Create from raw pointer and length
    ///
    /// # Safety
    /// - ptr must have been allocated by RGW (e.g., rgw_get_object)
    /// - len must be the actual length of the buffer
    /// - ptr must not be freed manually
    pub unsafe fn from_raw(ptr: *mut c_char, len: usize) -> Self {
        Self(ptr, len)
    }

    /// Get buffer as byte slice
    pub fn as_bytes(&self) -> &[u8] {
        if self.0.is_null() || self.1 == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.0 as *const u8, self.1) }
        }
    }

    /// Check if buffer is null/empty
    pub fn is_empty(&self) -> bool {
        self.0.is_null() || self.1 == 0
    }

    /// Get buffer length
    pub fn len(&self) -> usize {
        self.1
    }
}

impl Drop for RgwBuffer {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { ffi::rgw_free_buffer(self.0) }
        }
    }
}

/// RAII wrapper for C-allocated string
///
/// Automatically freed with `rgw_free_string` when dropped.
pub struct RgwString(*mut c_char);

impl RgwString {
    /// Create from raw pointer
    ///
    /// # Safety
    /// - ptr must have been allocated by RGW (e.g., etag return values)
    /// - ptr must be a valid null-terminated C string
    /// - ptr must not be freed manually
    pub unsafe fn from_raw(ptr: *mut c_char) -> Self {
        Self(ptr)
    }

    /// Get string as Rust &str
    ///
    /// Returns None if:
    /// - Pointer is null
    /// - String is not valid UTF-8
    pub fn as_str(&self) -> Option<&str> {
        if self.0.is_null() {
            None
        } else {
            unsafe { CStr::from_ptr(self.0).to_str().ok() }
        }
    }

    /// Check if string is null
    pub fn is_null(&self) -> bool {
        self.0.is_null()
    }

    /// Convert to owned String
    ///
    /// Returns None if pointer is null or string is not valid UTF-8
    pub fn to_string(&self) -> Option<String> {
        self.as_str().map(|s| s.to_string())
    }
}

impl Drop for RgwString {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { ffi::rgw_free_string(self.0) }
        }
    }
}

/// RAII wrapper for list result
///
/// Automatically freed with `rgw_list_result_free` when dropped.
pub struct RgwListResultWrapper(ffi::RGWListResult);

impl RgwListResultWrapper {
    /// Create from raw result structure
    ///
    /// # Safety
    /// - result must have been populated by rgw_list_objects
    /// - result must not be freed manually
    pub unsafe fn from_raw(result: ffi::RGWListResult) -> Self {
        Self(result)
    }

    /// Get reference to inner result
    pub fn inner(&self) -> &ffi::RGWListResult {
        &self.0
    }

    /// Get mutable reference to inner result
    pub fn inner_mut(&mut self) -> &mut ffi::RGWListResult {
        &mut self.0
    }

    /// Get number of objects in result
    pub fn num_objects(&self) -> u32 {
        self.0.num_objects
    }

    /// Get number of common prefixes
    pub fn num_common_prefixes(&self) -> u32 {
        self.0.num_common_prefixes
    }

    /// Check if there are more results (pagination needed)
    pub fn is_truncated(&self) -> bool {
        self.0.is_truncated != 0
    }

    /// Get next marker for pagination
    ///
    /// Returns None if no more results or marker is null
    pub fn next_marker(&self) -> Option<&str> {
        if self.0.next_marker.is_null() {
            None
        } else {
            unsafe { CStr::from_ptr(self.0.next_marker).to_str().ok() }
        }
    }

    /// Iterate over object entries
    ///
    /// # Safety
    /// Caller must ensure the wrapper is not dropped while iterator is in use
    pub fn entries(&self) -> impl Iterator<Item = &ffi::RGWObjectEntry> {
        if self.0.entries.is_null() {
            [].iter()
        } else {
            unsafe {
                std::slice::from_raw_parts(self.0.entries, self.0.num_objects as usize).iter()
            }
        }
    }

    /// Iterate over common prefixes
    ///
    /// # Safety
    /// Caller must ensure the wrapper is not dropped while iterator is in use
    pub fn common_prefixes(&self) -> Vec<Option<&str>> {
        if self.0.common_prefixes.is_null() {
            Vec::new()
        } else {
            let prefixes = unsafe {
                std::slice::from_raw_parts(
                    self.0.common_prefixes,
                    self.0.num_common_prefixes as usize,
                )
            };
            prefixes
                .iter()
                .map(|&prefix_ptr| {
                    if prefix_ptr.is_null() {
                        None
                    } else {
                        unsafe { CStr::from_ptr(prefix_ptr).to_str().ok() }
                    }
                })
                .collect()
        }
    }
}

impl Drop for RgwListResultWrapper {
    fn drop(&mut self) {
        unsafe { ffi::rgw_list_result_free(&mut self.0 as *mut _) }
    }
}

/// RAII wrapper for object metadata
///
/// Automatically frees etag string when dropped.
pub struct RgwObjectMetaWrapper(ffi::RGWObjectMeta);

impl RgwObjectMetaWrapper {
    /// Create from raw metadata structure
    ///
    /// # Safety
    /// - meta must have been populated by an RGW function
    /// - meta.etag (if non-null) must be a valid C string allocated by RGW
    pub unsafe fn from_raw(meta: ffi::RGWObjectMeta) -> Self {
        Self(meta)
    }

    /// Get object size in bytes
    pub fn size(&self) -> u64 {
        self.0.size
    }

    /// Get modification time as (seconds, nanoseconds) tuple
    pub fn mtime(&self) -> (i64, i64) {
        (self.0.mtime_sec, self.0.mtime_nsec)
    }

    /// Get ETag as Rust string
    ///
    /// Returns None if etag is null or not valid UTF-8
    pub fn etag(&self) -> Option<&str> {
        if self.0.etag.is_null() {
            None
        } else {
            unsafe { CStr::from_ptr(self.0.etag).to_str().ok() }
        }
    }

    /// Get reference to inner metadata
    pub fn inner(&self) -> &ffi::RGWObjectMeta {
        &self.0
    }
}

impl Drop for RgwObjectMetaWrapper {
    fn drop(&mut self) {
        if !self.0.etag.is_null() {
            unsafe { ffi::rgw_free_string(self.0.etag) }
        }
    }
}

/// RAII wrapper for range results
///
/// Automatically freed with `rgw_free_ranges` when dropped.
pub struct RgwRangeResultsWrapper {
    results: *mut ffi::RGWRangeResult,
    count: u32,
}

impl RgwRangeResultsWrapper {
    /// Create from raw results pointer and count
    ///
    /// # Safety
    /// - results must have been allocated by rgw_get_object_ranges
    /// - count must match the actual number of range results
    pub unsafe fn from_raw(results: *mut ffi::RGWRangeResult, count: u32) -> Self {
        Self { results, count }
    }

    /// Get number of range results
    pub fn count(&self) -> u32 {
        self.count
    }

    /// Iterate over range results
    ///
    /// # Safety
    /// Caller must ensure the wrapper is not dropped while iterator is in use
    pub fn iter(&self) -> impl Iterator<Item = &ffi::RGWRangeResult> {
        if self.results.is_null() {
            [].iter()
        } else {
            unsafe { std::slice::from_raw_parts(self.results, self.count as usize).iter() }
        }
    }

    /// Get a single range result by index
    ///
    /// Returns None if index is out of bounds or results is null
    pub fn get(&self, index: usize) -> Option<&ffi::RGWRangeResult> {
        if self.results.is_null() || index >= self.count as usize {
            None
        } else {
            unsafe { Some(&*self.results.add(index)) }
        }
    }
}

impl Drop for RgwRangeResultsWrapper {
    fn drop(&mut self) {
        if !self.results.is_null() {
            unsafe { ffi::rgw_free_ranges(self.results, self.count) }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;

    #[test]
    fn test_rgw_buffer_empty() {
        let buffer = unsafe { RgwBuffer::from_raw(ptr::null_mut(), 0) };
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.as_bytes(), &[]);
    }

    #[test]
    fn test_rgw_string_null() {
        let string = unsafe { RgwString::from_raw(ptr::null_mut()) };
        assert!(string.is_null());
        assert!(string.as_str().is_none());
        assert!(string.to_string().is_none());
    }
}
