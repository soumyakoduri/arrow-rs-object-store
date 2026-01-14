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

//! SAL Multipart Upload Implementation
//!
//! This module provides native S3-compatible multipart upload support using SAL.

use super::{client::SalClient, ffi};
use crate::{path::Path, Error, MultipartUpload, PutPayload, PutResult, Result, UploadPart};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;
use tokio::task;

/// SAL Multipart Upload
pub struct SalMultipartUpload {
    client: SalClient,
    location: Path,
    upload_id: String,
    parts: Arc<Mutex<Vec<(usize, String)>>>, // (part_num, etag)
}

impl SalMultipartUpload {
    /// Create a new multipart upload
    pub(crate) async fn new(
        client: SalClient,
        location: Path,
    ) -> Result<Self> {
        let location_clone = location.clone();
        let conn = client.get_connection()?;

        // Initiate multipart upload
        let upload_id = task::spawn_blocking(move || {
            unsafe {
                let bucket_cstr = CString::new(conn.bucket_name.as_str())
                    .map_err(|e| Error::Generic {
                        store: "SAL",
                        source: Box::new(e),
                    })?;
                let bucket = ffi::sal_get_bucket(
                    conn.driver,
                    ptr::null_mut(),
                    bucket_cstr.as_ptr(),
                    ptr::null(),
                );

                if bucket.is_null() {
                    return Err(Error::NotFound {
                        path: conn.bucket_name.clone(),
                        source: "Bucket not found".into(),
                    });
                }

                let key_cstr = CString::new(location_clone.as_ref())
                    .map_err(|e| Error::Generic {
                        store: "SAL",
                        source: Box::new(e),
                    })?;

                let mut upload_id_ptr: *mut c_char = ptr::null_mut();

                let upload = ffi::sal_init_multipart(
                    conn.driver,
                    ptr::null_mut(),
                    bucket,
                    key_cstr.as_ptr(),
                    &mut upload_id_ptr,
                );

                ffi::sal_destroy_bucket(bucket);

                if upload.is_null() {
                    return Err(Error::Generic {
                        store: "SAL",
                        source: "Failed to initiate multipart upload".into(),
                    });
                }

                let upload_id = if !upload_id_ptr.is_null() {
                    let id = CStr::from_ptr(upload_id_ptr)
                        .to_string_lossy()
                        .to_string();
                    ffi::sal_free_upload_id(upload_id_ptr);
                    id
                } else {
                    // Generate a fallback upload ID
                    uuid::Uuid::new_v4().to_string()
                };

                // Cleanup upload handle (we'll recreate per part)
                ffi::sal_destroy_multipart(upload);

                Ok::<String, Error>(upload_id)
            }
        })
        .await
        .map_err(|e| Error::Generic {
            store: "SAL",
            source: Box::new(e),
        })??;

        Ok(Self {
            client,
            location,
            upload_id,
            parts: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

#[async_trait]
impl MultipartUpload for SalMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let client = self.client.clone();
        let location = self.location.clone();
        let upload_id = self.upload_id.clone();
        let parts = Arc::clone(&self.parts);

        // Get next part number
        let part_num = {
            let parts_guard = parts.lock();
            parts_guard.len() + 1
        };

        Box::pin(async move {
            // Collect data
            let bytes: Bytes = data.iter().fold(Bytes::new(), |mut acc, chunk| {
                let mut new_buf = BytesMut::with_capacity(acc.len() + chunk.len());
                new_buf.extend_from_slice(&acc);
                new_buf.extend_from_slice(chunk);
                new_buf.freeze()
            });

            let conn = client.get_connection()?;

            task::spawn_blocking(move || {
                unsafe {
                    // Get bucket
                    let bucket_cstr = CString::new(conn.bucket_name.as_str())
                        .map_err(|e| Error::Generic {
                            store: "SAL",
                            source: Box::new(e),
                        })?;
                    let bucket = ffi::sal_get_bucket(
                        conn.driver,
                        ptr::null_mut(),
                        bucket_cstr.as_ptr(),
                        ptr::null(),
                    );

                    if bucket.is_null() {
                        return Err(Error::NotFound {
                            path: conn.bucket_name.clone(),
                            source: "Bucket not found".into(),
                        });
                    }

                    let key_cstr = CString::new(location.as_ref())
                        .map_err(|e| Error::Generic {
                            store: "SAL",
                            source: Box::new(e),
                        })?;

                    // Re-initialize multipart for this part
                    let mut upload_id_ptr: *mut c_char = ptr::null_mut();
                    let upload = ffi::sal_init_multipart(
                        conn.driver,
                        ptr::null_mut(),
                        bucket,
                        key_cstr.as_ptr(),
                        &mut upload_id_ptr,
                    );

                    if !upload_id_ptr.is_null() {
                        ffi::sal_free_upload_id(upload_id_ptr);
                    }

                    if upload.is_null() {
                        ffi::sal_destroy_bucket(bucket);
                        return Err(Error::Generic {
                            store: "SAL",
                            source: "Failed to re-init multipart".into(),
                        });
                    }

                    // Upload part
                    let mut etag_ptr: *mut c_char = ptr::null_mut();
                    let ret = ffi::sal_upload_part(
                        upload,
                        ptr::null_mut(),
                        part_num as i32,
                        bytes.as_ptr() as *const c_char,
                        bytes.len() as u64,
                        &mut etag_ptr,
                    );

                    ffi::sal_destroy_multipart(upload);
                    ffi::sal_destroy_bucket(bucket);

                    if ret < 0 {
                        return Err(ffi::map_sal_error(ret, "sal_upload_part"));
                    }

                    // Get ETag
                    let etag = if !etag_ptr.is_null() {
                        let etag_str = CStr::from_ptr(etag_ptr)
                            .to_string_lossy()
                            .to_string();
                        ffi::sal_free_etag(etag_ptr);
                        etag_str
                    } else {
                        format!("part-{}", part_num)
                    };

                    // Store part metadata
                    {
                        let mut parts_guard = parts.lock();
                        parts_guard.push((part_num, etag));
                    }

                    Ok(())
                }
            })
            .await
            .map_err(|e| Error::Generic {
                store: "SAL",
                source: Box::new(e),
            })??;

            Ok(())
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let client = self.client.clone();
        let location = self.location.clone();
        let upload_id = self.upload_id.clone();

        // Get all parts
        let parts = {
            let parts_guard = self.parts.lock();
            parts_guard.clone()
        };

        if parts.is_empty() {
            return Err(Error::Generic {
                store: "SAL",
                source: "No parts uploaded".into(),
            });
        }

        // Sort by part number
        let mut sorted_parts = parts;
        sorted_parts.sort_by_key(|(num, _)| *num);

        let conn = client.get_connection()?;

        task::spawn_blocking(move || {
            unsafe {
                // Get bucket
                let bucket_cstr = CString::new(conn.bucket_name.as_str())
                    .map_err(|e| Error::Generic {
                        store: "SAL",
                        source: Box::new(e),
                    })?;
                let bucket = ffi::sal_get_bucket(
                    conn.driver,
                    ptr::null_mut(),
                    bucket_cstr.as_ptr(),
                    ptr::null(),
                );

                if bucket.is_null() {
                    return Err(Error::NotFound {
                        path: conn.bucket_name.clone(),
                        source: "Bucket not found".into(),
                    });
                }

                let key_cstr = CString::new(location.as_ref())
                    .map_err(|e| Error::Generic {
                        store: "SAL",
                        source: Box::new(e),
                    })?;

                // Re-initialize multipart for completion
                let mut upload_id_ptr: *mut c_char = ptr::null_mut();
                let upload = ffi::sal_init_multipart(
                    conn.driver,
                    ptr::null_mut(),
                    bucket,
                    key_cstr.as_ptr(),
                    &mut upload_id_ptr,
                );

                if !upload_id_ptr.is_null() {
                    ffi::sal_free_upload_id(upload_id_ptr);
                }

                if upload.is_null() {
                    ffi::sal_destroy_bucket(bucket);
                    return Err(Error::Generic {
                        store: "SAL",
                        source: "Failed to re-init multipart for completion".into(),
                    });
                }

                // Build ETags array
                let etag_cstrings: Vec<CString> = sorted_parts
                    .iter()
                    .map(|(_, etag)| CString::new(etag.as_str()).unwrap())
                    .collect();

                let etag_ptrs: Vec<*const c_char> = etag_cstrings
                    .iter()
                    .map(|s| s.as_ptr())
                    .collect();

                // Complete multipart
                let mut final_etag_ptr: *mut c_char = ptr::null_mut();
                let ret = ffi::sal_complete_multipart(
                    upload,
                    ptr::null_mut(),
                    etag_ptrs.as_ptr(),
                    etag_ptrs.len() as i32,
                    &mut final_etag_ptr,
                );

                ffi::sal_destroy_multipart(upload);
                ffi::sal_destroy_bucket(bucket);

                if ret < 0 {
                    return Err(ffi::map_sal_error(ret, "sal_complete_multipart"));
                }

                let e_tag = if !final_etag_ptr.is_null() {
                    let etag_str = CStr::from_ptr(final_etag_ptr)
                        .to_string_lossy()
                        .to_string();
                    ffi::sal_free_etag(final_etag_ptr);
                    Some(etag_str)
                } else {
                    None
                };

                Ok(PutResult {
                    e_tag,
                    version: None,
                })
            }
        })
        .await
        .map_err(|e| Error::Generic {
            store: "SAL",
            source: Box::new(e),
        })?
    }

    async fn abort(&mut self) -> Result<()> {
        let client = self.client.clone();
        let location = self.location.clone();
        let upload_id = self.upload_id.clone();

        let conn = client.get_connection()?;

        task::spawn_blocking(move || {
            unsafe {
                // Get bucket
                let bucket_cstr = CString::new(conn.bucket_name.as_str())
                    .map_err(|e| Error::Generic {
                        store: "SAL",
                        source: Box::new(e),
                    })?;
                let bucket = ffi::sal_get_bucket(
                    conn.driver,
                    ptr::null_mut(),
                    bucket_cstr.as_ptr(),
                    ptr::null(),
                );

                if bucket.is_null() {
                    return Err(Error::NotFound {
                        path: conn.bucket_name.clone(),
                        source: "Bucket not found".into(),
                    });
                }

                let key_cstr = CString::new(location.as_ref())
                    .map_err(|e| Error::Generic {
                        store: "SAL",
                        source: Box::new(e),
                    })?;

                // Re-initialize multipart for abort
                let mut upload_id_ptr: *mut c_char = ptr::null_mut();
                let upload = ffi::sal_init_multipart(
                    conn.driver,
                    ptr::null_mut(),
                    bucket,
                    key_cstr.as_ptr(),
                    &mut upload_id_ptr,
                );

                if !upload_id_ptr.is_null() {
                    ffi::sal_free_upload_id(upload_id_ptr);
                }

                if upload.is_null() {
                    ffi::sal_destroy_bucket(bucket);
                    // Not an error if upload doesn't exist
                    return Ok(());
                }

                // Abort
                let ret = ffi::sal_abort_multipart(upload, ptr::null_mut());

                ffi::sal_destroy_multipart(upload);
                ffi::sal_destroy_bucket(bucket);

                if ret < 0 && ret != -2 {  // Ignore ENOENT
                    return Err(ffi::map_sal_error(ret, "sal_abort_multipart"));
                }

                Ok(())
            }
        })
        .await
        .map_err(|e| Error::Generic {
            store: "SAL",
            source: Box::new(e),
        })?
    }
}

impl std::fmt::Debug for SalMultipartUpload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SalMultipartUpload")
            .field("location", &self.location)
            .field("upload_id", &self.upload_id)
            .field("parts_count", &self.parts.lock().len())
            .finish()
    }
}
