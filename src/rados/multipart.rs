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

//! Multipart upload implementation for RADOS
//!
//! Since RADOS doesn't have native multipart upload support like S3,
//! we implement it using temporary objects and concatenation:
//!
//! 1. Each part is written to a temporary object: `{upload_id}.part.{part_number}`
//! 2. On complete, parts are concatenated into the final object using rados_append
//! 3. On abort, temporary part objects are deleted

use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use tokio::task;
use uuid::Uuid;

use crate::{Error, MultipartId, MultipartUpload, Path, PutPartPayload, Result, UploadPart};

use super::client::RadosClient;
use super::ffi;

/// Multipart upload state
#[derive(Debug)]
pub struct RadosMultipartUpload {
    client: RadosClient,
    location: Path,
    upload_id: String,
    parts: Arc<Mutex<Vec<UploadPart>>>,
}

impl RadosMultipartUpload {
    /// Create a new multipart upload
    pub fn new(client: RadosClient, location: Path) -> Result<Self> {
        let upload_id = Uuid::new_v4().to_string();

        Ok(Self {
            client,
            location,
            upload_id,
            parts: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Get the temporary object name for a part
    fn part_name(&self, part_idx: usize) -> String {
        format!("{}.part.{}", self.upload_id, part_idx)
    }

    /// Delete a temporary part object
    async fn delete_part(&self, part_idx: usize) -> Result<()> {
        let part_name = self.part_name(part_idx);
        let conn = self.client.get_connection()?;

        task::spawn_blocking(move || {
            let oid = CString::new(part_name.as_str()).map_err(|e| Error::Generic {
                store: "RADOS",
                source: Box::new(e),
            })?;

            unsafe {
                ffi::rados_remove(conn.ioctx, oid.as_ptr());
                // Ignore errors on cleanup
            }

            Ok(())
        })
        .await
        .map_err(|e| Error::Generic {
            store: "RADOS",
            source: Box::new(e),
        })?
    }
}

#[async_trait]
impl MultipartUpload for RadosMultipartUpload {
    fn put_part(&mut self, payload: PutPartPayload) -> Result<UploadPart> {
        // Collect the payload data
        let data: Bytes = payload.iter().fold(Bytes::new(), |mut acc, chunk| {
            let mut new_buf = bytes::BytesMut::with_capacity(acc.len() + chunk.len());
            new_buf.extend_from_slice(&acc);
            new_buf.extend_from_slice(chunk);
            new_buf.freeze()
        });

        let part_idx = {
            let mut parts = self.parts.lock();
            parts.len()
        };

        let part_name = self.part_name(part_idx);
        let conn = self.client.get_connection()?;
        let parts = Arc::clone(&self.parts);
        let content_id = format!("part-{}", part_idx);

        // Spawn blocking task to write the part
        let handle = task::spawn_blocking(move || {
            let oid = CString::new(part_name.as_str()).map_err(|e| Error::Generic {
                store: "RADOS",
                source: Box::new(e),
            })?;

            unsafe {
                let ret = ffi::rados_write_full(
                    conn.ioctx,
                    oid.as_ptr(),
                    data.as_ptr() as *const c_char,
                    data.len(),
                );

                if ret < 0 {
                    return Err(Error::Generic {
                        store: "RADOS",
                        source: format!("rados_write_full failed: {}", ret).into(),
                    });
                }
            }

            let upload_part = UploadPart {
                content_id: content_id.clone(),
            };

            // Add to parts list
            parts.lock().push(upload_part.clone());

            Ok(upload_part)
        });

        // Convert the blocking task to a sync result
        // Note: This is a simplification. In production, you might want to handle this asynchronously
        futures::executor::block_on(async {
            handle.await.map_err(|e| Error::Generic {
                store: "RADOS",
                source: Box::new(e),
            })?
        })
    }

    async fn complete(&mut self) -> Result<()> {
        let parts = self.parts.lock().clone();
        let location = self.location.clone();
        let upload_id = self.upload_id.clone();
        let pool_name = self.client.pool_name().to_string();
        let conn = self.client.get_connection()?;

        task::spawn_blocking(move || {
            let final_oid = CString::new(location.as_ref()).map_err(|e| Error::Generic {
                store: "RADOS",
                source: Box::new(e),
            })?;

            unsafe {
                // Write the first part as the base object
                if !parts.is_empty() {
                    let part0_name = format!("{}.part.0", upload_id);
                    let part0_oid = CString::new(part0_name.as_str()).map_err(|e| Error::Generic {
                        store: "RADOS",
                        source: Box::new(e),
                    })?;

                    // Read first part
                    let mut size: u64 = 0;
                    let ret = ffi::rados_stat(conn.ioctx, part0_oid.as_ptr(), &mut size, ptr::null_mut());
                    if ret < 0 {
                        return Err(Error::Generic {
                            store: "RADOS",
                            source: format!("Failed to stat part 0: {}", ret).into(),
                        });
                    }

                    let mut buffer = vec![0u8; size as usize];
                    let ret = ffi::rados_read(
                        conn.ioctx,
                        part0_oid.as_ptr(),
                        buffer.as_mut_ptr() as *mut c_char,
                        size as usize,
                        0,
                    );

                    if ret < 0 {
                        return Err(Error::Generic {
                            store: "RADOS",
                            source: format!("Failed to read part 0: {}", ret).into(),
                        });
                    }

                    // Write as final object
                    let ret = ffi::rados_write_full(
                        conn.ioctx,
                        final_oid.as_ptr(),
                        buffer.as_ptr() as *const c_char,
                        buffer.len(),
                    );

                    if ret < 0 {
                        return Err(Error::Generic {
                            store: "RADOS",
                            source: format!("Failed to write final object: {}", ret).into(),
                        });
                    }

                    // Append remaining parts
                    for i in 1..parts.len() {
                        let part_name = format!("{}.part.{}", upload_id, i);
                        let part_oid = CString::new(part_name.as_str()).map_err(|e| Error::Generic {
                            store: "RADOS",
                            source: Box::new(e),
                        })?;

                        // Get part size
                        let mut part_size: u64 = 0;
                        let ret = ffi::rados_stat(conn.ioctx, part_oid.as_ptr(), &mut part_size, ptr::null_mut());
                        if ret < 0 {
                            return Err(Error::Generic {
                                store: "RADOS",
                                source: format!("Failed to stat part {}: {}", i, ret).into(),
                            });
                        }

                        // Read part
                        let mut part_buffer = vec![0u8; part_size as usize];
                        let ret = ffi::rados_read(
                            conn.ioctx,
                            part_oid.as_ptr(),
                            part_buffer.as_mut_ptr() as *mut c_char,
                            part_size as usize,
                            0,
                        );

                        if ret < 0 {
                            return Err(Error::Generic {
                                store: "RADOS",
                                source: format!("Failed to read part {}: {}", i, ret).into(),
                            });
                        }

                        // Append to final object
                        let ret = ffi::rados_append(
                            conn.ioctx,
                            final_oid.as_ptr(),
                            part_buffer.as_ptr() as *const c_char,
                            part_buffer.len(),
                        );

                        if ret < 0 {
                            return Err(Error::Generic {
                                store: "RADOS",
                                source: format!("Failed to append part {}: {}", i, ret).into(),
                            });
                        }
                    }

                    // Clean up temporary parts
                    for i in 0..parts.len() {
                        let part_name = format!("{}.part.{}", upload_id, i);
                        let part_oid = CString::new(part_name.as_str()).map_err(|e| Error::Generic {
                            store: "RADOS",
                            source: Box::new(e),
                        })?;

                        ffi::rados_remove(conn.ioctx, part_oid.as_ptr());
                        // Ignore cleanup errors
                    }
                }

                Ok(())
            }
        })
        .await
        .map_err(|e| Error::Generic {
            store: "RADOS",
            source: Box::new(e),
        })?
    }

    async fn abort(&mut self) -> Result<()> {
        let parts = self.parts.lock().clone();

        // Delete all temporary parts
        for i in 0..parts.len() {
            self.delete_part(i).await?;
        }

        Ok(())
    }
}
