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

//! Integration tests for the RGW backend
//!
//! # Compilation Requirements
//!
//! To compile these tests, you must have librgw libraries available and linked.
//! The build.rs script will automatically link against the local ceph build.
//! Set CEPH_PATH environment variable if your ceph repo is not at ../ceph
//!
//! ```bash
//! CEPH_PATH=/path/to/ceph cargo test --features rgw --no-run
//! ```
//!
//! # Runtime Requirements
//!
//! These tests require an actual RGW setup with initialized driver and DPP pointers.
//! They are marked with #[ignore] by default and must be run explicitly with:
//! `cargo test --features rgw -- --ignored`
//!
//! To run these tests, you must:
//! 1. Have Ceph RGW installed and running
//! 2. Initialize an RGW driver using the C API
//! 3. Obtain valid driver_ptr and dpp_ptr pointers
//! 4. Set environment variables:
//!    - RGW_DRIVER_PTR: Hex-encoded pointer to RGW driver
//!    - RGW_DPP_PTR: Hex-encoded pointer to DPP
//!    - RGW_TEST_BUCKET: Bucket name for testing
//!
//! Example C code for initialization (not included in these tests):
//! ```c
//! rgw_driver_t driver;
//! dout_prefix_provider dpp;
//! rgw_driver_init(&driver, &dpp);
//! printf("DRIVER_PTR=%p\n", driver);
//! printf("DPP_PTR=%p\n", &dpp);
//! ```

#[cfg(feature = "rgw")]
mod rgw_tests {
    use bytes::Bytes;
    use futures::StreamExt;
    use object_store::{
        path::Path, GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutPayload,
    };
    use object_store::rgw::RgwObjectStoreBuilder;
    use std::os::raw::c_void;

    /// Helper to get test configuration from environment
    /// Returns None if required environment variables are not set
    fn get_test_config() -> Option<(*mut c_void, *const c_void, String)> {
        let driver_ptr = std::env::var("RGW_DRIVER_PTR").ok()?;
        let dpp_ptr = std::env::var("RGW_DPP_PTR").ok()?;
        let bucket = std::env::var("RGW_TEST_BUCKET").ok()?;

        // Parse hex strings to pointers
        let driver = usize::from_str_radix(driver_ptr.trim_start_matches("0x"), 16).ok()? as *mut c_void;
        let dpp = usize::from_str_radix(dpp_ptr.trim_start_matches("0x"), 16).ok()? as *const c_void;

        Some((driver, dpp, bucket))
    }

    /// Helper to create a test store
    /// Returns None if environment is not configured
    fn create_test_store() -> Option<impl ObjectStore> {
        let (driver_ptr, dpp_ptr, bucket) = get_test_config()?;

        unsafe {
            RgwObjectStoreBuilder::new(driver_ptr, dpp_ptr)
                .with_bucket(bucket)
                .build()
                .ok()
        }
    }

    #[tokio::test]
    #[ignore = "Requires actual RGW setup with driver/DPP pointers"]
    async fn test_put_get_delete() {
        let store = create_test_store().expect("RGW test environment not configured");
        let path = Path::from("test/rgw_integration_test.txt");
        let data = Bytes::from("Hello RGW!");

        // PUT object and verify ETag is returned
        let put_result = store.put(&path, data.clone().into()).await.unwrap();
        assert!(put_result.e_tag.is_some(), "PUT should return an ETag");

        // GET object
        let result = store.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes, data);

        // DELETE object
        store.delete(&path).await.unwrap();

        // Verify deletion
        assert!(store.get(&path).await.is_err());
    }

    #[tokio::test]
    #[ignore = "Requires actual RGW setup with driver/DPP pointers"]
    async fn test_etag_returned_and_consistent() {
        let store = create_test_store().expect("RGW test environment not configured");
        let path = Path::from("test/rgw_etag_test.txt");
        let data = Bytes::from("test data for etag");

        // PUT object and get ETag from PUT result
        let put_result = store.put(&path, data.clone().into()).await.unwrap();
        let put_etag = put_result.e_tag.expect("PUT should return an ETag");

        // HEAD object and verify ETag matches
        let meta = store.head(&path).await.unwrap();
        let head_etag = meta.e_tag.expect("HEAD should return an ETag");
        assert_eq!(put_etag, head_etag, "ETags from PUT and HEAD should match");

        // GET object and verify ETag matches
        let get_result = store.get(&path).await.unwrap();
        let get_etag = get_result.meta.e_tag.expect("GET should return an ETag");
        assert_eq!(put_etag, get_etag, "ETags from PUT and GET should match");

        // PUT different content and verify ETag changes
        let data2 = Bytes::from("different content");
        let put_result2 = store.put(&path, data2.into()).await.unwrap();
        let new_etag = put_result2.e_tag.expect("Second PUT should return an ETag");
        assert_ne!(put_etag, new_etag, "ETag should change when content changes");

        // Cleanup
        store.delete(&path).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "Requires actual RGW setup with driver/DPP pointers"]
    async fn test_conditional_get_with_etag() {
        let store = create_test_store().expect("RGW test environment not configured");
        let path = Path::from("test/rgw_conditional_get.txt");
        let data = Bytes::from("test data");

        // PUT object
        let put_result = store.put(&path, data.clone().into()).await.unwrap();
        let etag = put_result.e_tag.expect("PUT should return an ETag");

        // Test if_match with matching ETag (should succeed)
        let opts = GetOptions {
            if_match: Some(etag.clone()),
            ..Default::default()
        };
        let result = store.get_opts(&path, opts).await;
        assert!(result.is_ok(), "GET with matching if_match should succeed");

        // Test if_match with non-matching ETag (should fail with Precondition error)
        let opts = GetOptions {
            if_match: Some("invalid-etag".to_string()),
            ..Default::default()
        };
        let result = store.get_opts(&path, opts).await;
        assert!(result.is_err(), "GET with non-matching if_match should fail");

        // Test if_none_match with matching ETag (should fail with NotModified error)
        let opts = GetOptions {
            if_none_match: Some(etag.clone()),
            ..Default::default()
        };
        let result = store.get_opts(&path, opts).await;
        assert!(result.is_err(), "GET with matching if_none_match should fail");

        // Test if_none_match with non-matching ETag (should succeed)
        let opts = GetOptions {
            if_none_match: Some("invalid-etag".to_string()),
            ..Default::default()
        };
        let result = store.get_opts(&path, opts).await;
        assert!(result.is_ok(), "GET with non-matching if_none_match should succeed");

        // Cleanup
        store.delete(&path).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "Requires actual RGW setup with driver/DPP pointers"]
    async fn test_get_range() {
        let store = create_test_store().expect("RGW test environment not configured");
        let path = Path::from("test/rgw_range_test.txt");
        let data = Bytes::from("0123456789ABCDEF");

        // PUT object
        store.put(&path, data.clone().into()).await.unwrap();

        // GET range [5..10]
        let result = store
            .get_opts(
                &path,
                GetOptions::new().with_range(Some(GetRange::Bounded(5..10))),
            )
            .await
            .unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from("56789"));

        // GET with offset
        let result = store
            .get_opts(&path, GetOptions::new().with_range(Some(GetRange::Offset(10))))
            .await
            .unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from("ABCDEF"));

        // GET suffix
        let result = store
            .get_opts(&path, GetOptions::new().with_range(Some(GetRange::Suffix(4))))
            .await
            .unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes, Bytes::from("CDEF"));

        // Cleanup
        store.delete(&path).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "Requires actual RGW setup with driver/DPP pointers"]
    async fn test_head() {
        let store = create_test_store().expect("RGW test environment not configured");
        let path = Path::from("test/rgw_head_test.txt");
        let data = Bytes::from("metadata test");

        // PUT object
        store.put(&path, data.clone().into()).await.unwrap();

        // HEAD request
        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.location, path);
        assert_eq!(meta.size, data.len() as u64);
        assert!(meta.e_tag.is_some(), "HEAD should return an ETag");

        // Cleanup
        store.delete(&path).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "Requires actual RGW setup with driver/DPP pointers"]
    async fn test_list() {
        let store = create_test_store().expect("RGW test environment not configured");
        let prefix = Path::from("test/list_test/");

        // PUT multiple objects
        let files = vec![
            "file1.txt",
            "file2.txt",
            "file3.txt",
            "subdir/file4.txt",
        ];

        for file in &files {
            let path = prefix.child(*file);
            store
                .put(&path, Bytes::from(format!("content of {}", file)).into())
                .await
                .unwrap();
        }

        // LIST all objects
        let mut objects = store.list(Some(&prefix)).collect::<Vec<_>>().await;
        objects.sort_by(|a, b| {
            let a = a.as_ref().map(|m| m.location.as_ref()).unwrap_or("");
            let b = b.as_ref().map(|m| m.location.as_ref()).unwrap_or("");
            a.cmp(b)
        });

        assert_eq!(objects.len(), 4);

        // Verify all objects have ETags in list results
        for obj_result in &objects {
            let obj = obj_result.as_ref().unwrap();
            assert!(obj.e_tag.is_some(), "Listed object should have an ETag");
        }

        // Cleanup
        for file in &files {
            let path = prefix.child(*file);
            store.delete(&path).await.unwrap();
        }
    }

    #[tokio::test]
    #[ignore = "Requires actual RGW setup with driver/DPP pointers"]
    async fn test_list_with_delimiter() {
        let store = create_test_store().expect("RGW test environment not configured");
        let prefix = Path::from("test/delimiter_test/");

        // PUT objects in different "directories"
        let files = vec![
            "root_file.txt",
            "dir1/file1.txt",
            "dir1/file2.txt",
            "dir2/file3.txt",
        ];

        for file in &files {
            let path = prefix.child(*file);
            store.put(&path, Bytes::from("test").into()).await.unwrap();
        }

        // LIST with delimiter (should only show root_file and dir1/, dir2/ prefixes)
        let result = store.list_with_delimiter(Some(&prefix)).await.unwrap();

        assert_eq!(result.objects.len(), 1); // Only root_file.txt
        assert_eq!(result.common_prefixes.len(), 2); // dir1/ and dir2/

        // Cleanup
        for file in &files {
            let path = prefix.child(*file);
            store.delete(&path).await.unwrap();
        }
    }

    #[tokio::test]
    #[ignore = "Requires actual RGW setup with driver/DPP pointers"]
    async fn test_empty_object() {
        let store = create_test_store().expect("RGW test environment not configured");
        let path = Path::from("test/rgw_empty_test.txt");

        // PUT empty object
        let data = Bytes::new();
        let put_result = store.put(&path, data.clone().into()).await.unwrap();

        // Even empty objects should have ETags
        assert!(put_result.e_tag.is_some(), "Empty object should have an ETag");

        // GET empty object
        let result = store.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(bytes.len(), 0);

        // HEAD should show size 0 but still have ETag
        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.size, 0);
        assert!(meta.e_tag.is_some(), "Empty object HEAD should have an ETag");

        // Cleanup
        store.delete(&path).await.unwrap();
    }
}
