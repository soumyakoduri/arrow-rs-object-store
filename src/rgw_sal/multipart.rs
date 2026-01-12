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

//! Multipart upload implementation using SAL APIs

use std::ffi::{CString, CStr};
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use tokio::task;
use uuid::Uuid;

use crate::{
    Error, MultipartUpload, Path, PutPayload, PutResult, Result, UploadPart,
};

use super::client::SalClient;
use super::ffi;

/// SAL-based multipart upload implementation
///
/// This uses SAL's native multipart upload capabilities instead of
/// manually managing parts like the RADOS implementation.
pub struct SalMultipartUpload {
    client: SalClient,
    location: Path,
    upload_id: String,
    // Track uploaded parts
    parts: Arc<Mutex<Vec<(usize, String)>>>, // (part_number, etag)
}

impl SalMultipartUpload {
    /// Create a new multipart upload
    pub fn new(client: SalClient, location: Path) -> Result<Self> {
        let upload_id = Uuid::new_v4().to_string();

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

        // Get the next part number
        let part_num = {
            let parts_guard = parts.lock();
            parts_guard.len() + 1
        };

        Box::pin(async move {
            // Collect part data
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
                    let bucket = {
                        let bucket_cstr = CString::new(conn.bucket_name.as_str()).map_err(|e| Error::Generic {
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
                        bucket
                    };

                    // Create writer
                    let key_cstr = CString::new(location.as_ref()).map_err(|e| Error::Generic {
                        store: "SAL",
                        source: Box::new(e),
                    })?;

                    let writer = ffi::sal_create_writer(
                        conn.driver,
                        ptr::null_mut(),
                        bucket,
                        key_cstr.as_ptr(),
                    );

                    if writer.is_null() {
                        ffi::sal_destroy_bucket(bucket);
                        return Err(Error::Generic {
                            store: "SAL",
                            source: "Failed to create writer".into(),
                        });
                    }

                    // Initialize multipart upload
                    let upload_id_cstr = CString::new(upload_id.as_str()).map_err(|e| Error::Generic {
                        store: "SAL",
                        source: Box::new(e),
                    })?;

                    let multipart = ffi::sal_init_multipart(
                        writer,
                        ptr::null_mut(),
                        upload_id_cstr.as_ptr(),
                    );

                    if multipart.is_null() {
                        ffi::sal_destroy_writer(writer);
                        ffi::sal_destroy_bucket(bucket);
                        return Err(Error::Generic {
                            store: "SAL",
                            source: "Failed to initialize multipart upload".into(),
                        });
                    }

                    // Upload part
                    let mut etag_ptr: *mut c_char = ptr::null_mut();
                    let ret = ffi::sal_upload_part(
                        multipart,
                        ptr::null_mut(),
                        part_num as i32,
                        bytes.as_ptr() as *const c_char,
                        bytes.len() as u64,
                        &mut etag_ptr,
                    );

                    ffi::sal_destroy_multipart(multipart);
                    ffi::sal_destroy_writer(writer);
                    ffi::sal_destroy_bucket(bucket);

                    if ret < 0 {
                        return Err(Error::Generic {
                            store: "SAL",
                            source: format!("Failed to upload part {}: error code {}", part_num, ret).into(),
                        });
                    }

                    // Get ETag
                    let etag = if !etag_ptr.is_null() {
                        let etag_str = CStr::from_ptr(etag_ptr).to_string_lossy().to_string();
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
            })?
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

        // Sort parts by part number
        let mut sorted_parts = parts;
        sorted_parts.sort_by_key(|(num, _)| *num);

        let conn = client.get_connection()?;

        task::spawn_blocking(move || {
            unsafe {
                // Get bucket
                let bucket = {
                    let bucket_cstr = CString::new(conn.bucket_name.as_str()).map_err(|e| Error::Generic {
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
                    bucket
                };

                // Create writer
                let key_cstr = CString::new(location.as_ref()).map_err(|e| Error::Generic {
                    store: "SAL",
                    source: Box::new(e),
                })?;

                let writer = ffi::sal_create_writer(
                    conn.driver,
                    ptr::null_mut(),
                    bucket,
                    key_cstr.as_ptr(),
                );

                if writer.is_null() {
                    ffi::sal_destroy_bucket(bucket);
                    return Err(Error::Generic {
                        store: "SAL",
                        source: "Failed to create writer".into(),
                    });
                }

                // Initialize multipart upload
                let upload_id_cstr = CString::new(upload_id.as_str()).map_err(|e| Error::Generic {
                    store: "SAL",
                    source: Box::new(e),
                })?;

                let multipart = ffi::sal_init_multipart(
                    writer,
                    ptr::null_mut(),
                    upload_id_cstr.as_ptr(),
                );

                if multipart.is_null() {
                    ffi::sal_destroy_writer(writer);
                    ffi::sal_destroy_bucket(bucket);
                    return Err(Error::Generic {
                        store: "SAL",
                        source: "Failed to initialize multipart upload".into(),
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

                // Complete multipart upload
                let mut final_etag_ptr: *mut c_char = ptr::null_mut();
                let ret = ffi::sal_complete_multipart(
                    multipart,
                    ptr::null_mut(),
                    etag_ptrs.as_ptr(),
                    etag_ptrs.len() as i32,
                    &mut final_etag_ptr,
                );

                ffi::sal_destroy_multipart(multipart);
                ffi::sal_destroy_writer(writer);
                ffi::sal_destroy_bucket(bucket);

                if ret < 0 {
                    return Err(Error::Generic {
                        store: "SAL",
                        source: format!("Failed to complete multipart upload: error code {}", ret).into(),
                    });
                }

                // Get final ETag
                let e_tag = if !final_etag_ptr.is_null() {
                    let etag_str = CStr::from_ptr(final_etag_ptr).to_string_lossy().to_string();
                    ffi::sal_free_etag(final_etag_ptr);
                    Some(etag_str)
                } else {
                    None
                };

                Ok(PutResult { e_tag, version: None })
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
                let bucket = {
                    let bucket_cstr = CString::new(conn.bucket_name.as_str()).map_err(|e| Error::Generic {
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
                    bucket
                };

                // Create writer
                let key_cstr = CString::new(location.as_ref()).map_err(|e| Error::Generic {
                    store: "SAL",
                    source: Box::new(e),
                })?;

                let writer = ffi::sal_create_writer(
                    conn.driver,
                    ptr::null_mut(),
                    bucket,
                    key_cstr.as_ptr(),
                );

                if writer.is_null() {
                    ffi::sal_destroy_bucket(bucket);
                    return Err(Error::Generic {
                        store: "SAL",
                        source: "Failed to create writer".into(),
                    });
                }

                // Initialize multipart upload
                let upload_id_cstr = CString::new(upload_id.as_str()).map_err(|e| Error::Generic {
                    store: "SAL",
                    source: Box::new(e),
                })?;

                let multipart = ffi::sal_init_multipart(
                    writer,
                    ptr::null_mut(),
                    upload_id_cstr.as_ptr(),
                );

                if multipart.is_null() {
                    ffi::sal_destroy_writer(writer);
                    ffi::sal_destroy_bucket(bucket);
                    return Err(Error::Generic {
                        store: "SAL",
                        source: "Failed to initialize multipart upload".into(),
                    });
                }

                // Abort multipart upload
                let ret = ffi::sal_abort_multipart(multipart, ptr::null_mut());

                ffi::sal_destroy_multipart(multipart);
                ffi::sal_destroy_writer(writer);
                ffi::sal_destroy_bucket(bucket);

                if ret < 0 {
                    return Err(Error::Generic {
                        store: "SAL",
                        source: format!("Failed to abort multipart upload: error code {}", ret).into(),
                    });
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
            .field("bucket", &self.client.bucket_name())
            .field("location", &self.location)
            .field("upload_id", &self.upload_id)
            .field("parts_count", &self.parts.lock().len())
            .finish()
    }
}
