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

//! Configuration builder for RGW ObjectStore
//!
//! Provides a builder pattern API for constructing RGWObjectStore instances.

use std::os::raw::c_void;

use crate::rgw::{error::RgwError, RGWObjectStore};
use crate::Result;

/// Builder for RGW ObjectStore
///
/// This builder accepts pre-initialized RGW driver and DPP (DoutPrefixProvider) pointers
/// from external C code. The user is responsible for:
/// - Initializing the RGW driver (e.g., via rados_create and rados_connect)
/// - Ensuring the driver and DPP pointers remain valid for the lifetime of the ObjectStore
/// - Managing the lifecycle of the driver (cleanup when done)
///
/// # Example
///
/// ```ignore
/// use object_store::rgw::RGWObjectStoreBuilder;
///
/// // Obtain driver and dpp from external initialization code
/// // (not shown - this is the user's responsibility)
///
/// let store = unsafe {
///     RGWObjectStoreBuilder::new(driver, dpp)
///         .with_bucket("my-bucket")
///         .build()?
/// };
/// ```
#[derive(Debug, Clone)]
pub struct RGWObjectStoreBuilder {
    driver: *mut c_void,
    dpp: *const c_void,
    bucket_name: Option<String>,
}

impl RGWObjectStoreBuilder {
    /// Create a new builder with RGW driver and DPP pointers
    ///
    /// # Safety
    ///
    /// The caller must ensure:
    /// - `driver` points to a valid RGW Driver instance
    /// - `dpp` points to a valid DoutPrefixProvider instance (or is null)
    /// - Both pointers remain valid for the lifetime of the created ObjectStore
    /// - The driver is properly initialized and connected
    /// - The driver supports thread-safe operations (for Send+Sync)
    ///
    /// # Arguments
    ///
    /// * `driver` - Pointer to initialized RGW Driver (e.g., from librgw)
    /// * `dpp` - Pointer to DoutPrefixProvider for logging (can be null)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // In C/C++ code:
    /// // rgw::sal::Driver* driver = create_rados_driver(...);
    /// // DoutPrefixProvider* dpp = ...;
    ///
    /// // In Rust:
    /// let builder = unsafe {
    ///     RGWObjectStoreBuilder::new(driver, dpp)
    /// };
    /// ```
    pub unsafe fn new(driver: *mut c_void, dpp: *const c_void) -> Self {
        Self {
            driver,
            dpp,
            bucket_name: None,
        }
    }

    /// Set the bucket name
    ///
    /// This is required - build() will fail if no bucket is specified.
    ///
    /// # Arguments
    ///
    /// * `bucket_name` - The name of the RGW bucket to use for all operations
    ///
    /// # Example
    ///
    /// ```ignore
    /// let builder = unsafe { RGWObjectStoreBuilder::new(driver, dpp) }
    ///     .with_bucket("my-data-bucket");
    /// ```
    pub fn with_bucket(mut self, bucket_name: impl Into<String>) -> Self {
        self.bucket_name = Some(bucket_name.into());
        self
    }

    /// Build the RGWObjectStore
    ///
    /// # Returns
    ///
    /// Returns an error if:
    /// - No bucket name was specified
    /// - The driver pointer is null
    ///
    /// # Example
    ///
    /// ```ignore
    /// let store = unsafe {
    ///     RGWObjectStoreBuilder::new(driver, dpp)
    ///         .with_bucket("my-bucket")
    ///         .build()?
    /// };
    /// ```
    pub fn build(self) -> Result<RGWObjectStore> {
        // Validate driver pointer
        if self.driver.is_null() {
            return Err(RgwError::NullPointer.into());
        }

        // Bucket is required
        let bucket_name = self.bucket_name.ok_or_else(|| RgwError::MissingField("bucket_name"))?;

        Ok(RGWObjectStore {
            bucket_name,
            driver: self.driver,
            dpp: self.dpp,
        })
    }
}

// Safety: Builder contains raw pointers but they are Send/Sync if the underlying
// RGW driver is thread-safe (which is the user's responsibility to ensure)
unsafe impl Send for RGWObjectStoreBuilder {}
unsafe impl Sync for RGWObjectStoreBuilder {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;

    #[test]
    fn test_builder_requires_bucket() {
        // Create builder without bucket
        let builder = unsafe { RGWObjectStoreBuilder::new(1 as *mut c_void, ptr::null()) };

        // Build should fail
        assert!(builder.build().is_err());
    }

    #[test]
    fn test_builder_null_driver() {
        // Create builder with null driver
        let builder = unsafe { RGWObjectStoreBuilder::new(ptr::null_mut(), ptr::null()) }
            .with_bucket("test");

        // Build should fail
        assert!(builder.build().is_err());
    }

    #[test]
    #[ignore = "Requires actual RGW driver"]
    fn test_builder_with_valid_pointers() {
        // This test would require actual RGW initialization
        // Marked as ignore since we don't have a real driver in tests
    }
}
