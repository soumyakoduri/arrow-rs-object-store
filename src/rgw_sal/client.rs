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
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! SAL client implementation using RGW SAL FFI bindings

use std::ffi::{CString, CStr};
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::stream::{BoxStream, StreamExt};
use tokio::task;

use crate::{
    Error, GetOptions, GetResult, GetResultPayload, ListResult, MultipartId, MultipartUpload,
    ObjectMeta, Path, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    UpdateVersion,
};

use super::ffi::{self, CephContext, SalBucket, SalDriver};
use super::multipart::SalMultipartUpload;

/// SAL connection handle
///
/// This wraps the SAL driver, CephContext, and bucket handles.
/// It's designed to be thread-safe and can be cloned cheaply.
#[derive(Debug)]
pub(crate) struct SalConnection {
    pub(crate) cct: *mut CephContext,
    pub(crate) driver: *mut SalDriver,
    pub(crate) bucket_name: String,
}

// Safety: We ensure thread-safety by only accessing these pointers
// from blocking tasks and properly synchronizing access
unsafe impl Send for SalConnection {}
unsafe impl Sync for SalConnection {}

impl Drop for SalConnection {
    fn drop(&mut self) {
        unsafe {
            if !self.driver.is_null() {
                ffi::sal_destroy_driver(self.driver);
            }
            if !self.cct.is_null() {
                ffi::sal_destroy_ceph_context(self.cct);
            }
        }
    }
}

/// Client for interacting with Ceph via RGW SAL
#[derive(Debug, Clone)]
pub struct SalClient {
    cluster_name: String,
    user_name: String,
    conf_file: Option<String>,
    bucket_name: String,
    tenant: Option<String>,
    keyring: Option<String>,
    key: Option<String>,
    // Connection is created on-demand and cached
    connection: Arc<parking_lot::Mutex<Option<Arc<SalConnection>>>>,
}

impl SalClient {
    /// Create a new SalClient
    pub fn new(
        cluster_name: String,
        user_name: String,
        conf_file: Option<String>,
        bucket_name: String,
        tenant: Option<String>,
        keyring: Option<String>,
        key: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            cluster_name,
            user_name,
            conf_file,
            bucket_name,
            tenant,
            keyring,
            key,
            connection: Arc::new(parking_lot::Mutex::new(None)),
        })
    }

    /// Get the bucket name
    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    /// Get or create a SAL connection
    pub(crate) fn get_connection(&self) -> Result<Arc<SalConnection>> {
        let mut conn_guard = self.connection.lock();

        if let Some(conn) = conn_guard.as_ref() {
            return Ok(Arc::clone(conn));
        }

        // Create new connection
        let conn = self.create_connection()?;
        let arc_conn = Arc::new(conn);
        *conn_guard = Some(Arc::clone(&arc_conn));

        Ok(arc_conn)
    }

    /// Create a new SAL connection
    fn create_connection(&self) -> Result<SalConnection> {
        unsafe {
            // Create CephContext
            let cluster_cstr = CString::new(self.cluster_name.as_str()).map_err(|e| Error::Generic {
                store: "SAL",
                source: Box::new(e),
            })?;

            let user_cstr = CString::new(self.user_name.as_str()).map_err(|e| Error::Generic {
                store: "SAL",
                source: Box::new(e),
            })?;

            let conf_cstr = if let Some(ref conf_file) = self.conf_file {
                Some(CString::new(conf_file.as_str()).map_err(|e| Error::Generic {
                    store: "SAL",
                    source: Box::new(e),
                })?)
            } else {
                None
            };

            let cct = ffi::sal_create_ceph_context(
                cluster_cstr.as_ptr(),
                user_cstr.as_ptr(),
                conf_cstr.as_ref().map_or(ptr::null(), |s| s.as_ptr()),
            );

            if cct.is_null() {
                return Err(Error::Generic {
                    store: "SAL",
                    source: "Failed to create CephContext".into(),
                });
            }

            // Configure authentication
            if self.keyring.is_some() || self.key.is_some() {
                let keyring_cstr = if let Some(ref keyring) = self.keyring {
                    Some(CString::new(keyring.as_str()).map_err(|e| {
                        ffi::sal_destroy_ceph_context(cct);
                        Error::Generic {
                            store: "SAL",
                            source: Box::new(e),
                        }
                    })?)
                } else {
                    None
                };

                let key_cstr = if let Some(ref key) = self.key {
                    Some(CString::new(key.as_str()).map_err(|e| {
                        ffi::sal_destroy_ceph_context(cct);
                        Error::Generic {
                            store: "SAL",
                            source: Box::new(e),
                        }
                    })?)
                } else {
                    None
                };

                let ret = ffi::sal_configure_auth(
                    cct,
                    keyring_cstr.as_ref().map_or(ptr::null(), |s| s.as_ptr()),
                    key_cstr.as_ref().map_or(ptr::null(), |s| s.as_ptr()),
                );

                if ret < 0 {
                    ffi::sal_destroy_ceph_context(cct);
                    return Err(map_sal_error(ret, "sal_configure_auth"));
                }
            }

            // Create SAL Driver (RADOSStore backend)
            let driver = ffi::sal_create_rados_driver(cct, ptr::null_mut());

            if driver.is_null() {
                ffi::sal_destroy_ceph_context(cct);
                return Err(Error::Generic {
                    store: "SAL",
                    source: "Failed to create SAL driver".into(),
                });
            }

            Ok(SalConnection {
                cct,
                driver,
                bucket_name: self.bucket_name.clone(),
            })
        }
    }

    /// Get a bucket handle
    fn get_bucket(&self, conn: &SalConnection) -> Result<*mut SalBucket> {
        unsafe {
            let bucket_cstr = CString::new(conn.bucket_name.as_str()).map_err(|e| Error::Generic {
                store: "SAL",
                source: Box::new(e),
            })?;

            let tenant_cstr = if let Some(ref tenant) = self.tenant {
                Some(CString::new(tenant.as_str()).map_err(|e| Error::Generic {
                    store: "SAL",
                    source: Box::new(e),
                })?)
            } else {
                None
            };

            let bucket = ffi::sal_get_bucket(
                conn.driver,
                ptr::null_mut(),
                bucket_cstr.as_ptr(),
                tenant_cstr.as_ref().map_or(ptr::null(), |s| s.as_ptr()),
            );

            if bucket.is_null() {
                return Err(Error::NotFound {
                    path: conn.bucket_name.clone(),
                    source: "Bucket not found".into(),
                });
            }

            Ok(bucket)
        }
    }

    /// Put an object with options
    pub async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        // Collect all bytes from the payload into a single buffer
        let data: Bytes = payload.iter().fold(Bytes::new(), |mut acc, chunk| {
            let mut new_buf = BytesMut::with_capacity(acc.len() + chunk.len());
            new_buf.extend_from_slice(&acc);
            new_buf.extend_from_slice(chunk);
            new_buf.freeze()
        });

        let location = location.clone();
        let conn = self.get_connection()?;

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

                // Prepare writer
                let ret = ffi::sal_writer_prepare(writer, ptr::null_mut());
                if ret < 0 {
                    ffi::sal_destroy_writer(writer);
                    ffi::sal_destroy_bucket(bucket);
                    return Err(map_sal_error(ret, "sal_writer_prepare"));
                }

                // Check if object exists for PutMode handling
                if matches!(opts.mode, PutMode::Create) {
                    let obj = ffi::sal_get_object(bucket, ptr::null_mut(), key_cstr.as_ptr());
                    if !obj.is_null() {
                        let mut meta = std::mem::zeroed();
                        let meta_ret = ffi::sal_get_object_meta(obj, ptr::null_mut(), &mut meta);
                        ffi::sal_destroy_object(obj);

                        if meta_ret >= 0 {
                            ffi::sal_destroy_writer(writer);
                            ffi::sal_destroy_bucket(bucket);
                            return Err(Error::AlreadyExists {
                                path: location.to_string(),
                                source: "Object already exists".into(),
                            });
                        }
                    }
                }

                // Write data
                let ret = ffi::sal_writer_write(
                    writer,
                    ptr::null_mut(),
                    data.as_ptr() as *const c_char,
                    data.len() as u64,
                    0,
                );

                if ret < 0 {
                    ffi::sal_destroy_writer(writer);
                    ffi::sal_destroy_bucket(bucket);
                    return Err(map_sal_error(ret, "sal_writer_write"));
                }

                // Complete write
                let mut etag_ptr: *mut c_char = ptr::null_mut();
                let ret = ffi::sal_writer_complete(writer, ptr::null_mut(), &mut etag_ptr);

                ffi::sal_destroy_writer(writer);
                ffi::sal_destroy_bucket(bucket);

                if ret < 0 {
                    return Err(map_sal_error(ret, "sal_writer_complete"));
                }

                let e_tag = if !etag_ptr.is_null() {
                    let etag_str = CStr::from_ptr(etag_ptr).to_string_lossy().to_string();
                    ffi::sal_free_etag(etag_ptr);
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

    /// Get an object with options
    pub async fn get_opts(&self, location: &Path, opts: GetOptions) -> Result<GetResult> {
        let location = location.clone();
        let conn = self.get_connection()?;

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

                // Get object
                let key_cstr = CString::new(location.as_ref()).map_err(|e| Error::Generic {
                    store: "SAL",
                    source: Box::new(e),
                })?;

                let object = ffi::sal_get_object(bucket, ptr::null_mut(), key_cstr.as_ptr());

                if object.is_null() {
                    ffi::sal_destroy_bucket(bucket);
                    return Err(Error::NotFound {
                        path: location.to_string(),
                        source: "Object not found".into(),
                    });
                }

                // Get object metadata
                let mut meta: ffi::SalObjectMeta = std::mem::zeroed();
                let ret = ffi::sal_get_object_meta(object, ptr::null_mut(), &mut meta);

                if ret < 0 {
                    ffi::sal_destroy_object(object);
                    ffi::sal_destroy_bucket(bucket);
                    return Err(map_sal_error(ret, "sal_get_object_meta"));
                }

                // Handle range if specified
                let (offset, read_len) = if let Some(range) = opts.range {
                    let start = range.start.unwrap_or(0) as u64;
                    let end = range.end.map(|e| e as u64).unwrap_or(meta.size);
                    (start, end - start)
                } else {
                    (0, meta.size)
                };

                // Read object data
                let mut buffer_ptr: *mut c_char = ptr::null_mut();
                let mut bytes_read: u64 = 0;

                let ret = ffi::sal_read_object(
                    object,
                    ptr::null_mut(),
                    offset,
                    read_len,
                    &mut buffer_ptr,
                    &mut bytes_read,
                );

                if ret < 0 {
                    ffi::sal_destroy_object(object);
                    ffi::sal_destroy_bucket(bucket);
                    return Err(map_sal_error(ret, "sal_read_object"));
                }

                // Copy data to Rust Vec
                let buffer = if !buffer_ptr.is_null() && bytes_read > 0 {
                    let slice = std::slice::from_raw_parts(buffer_ptr as *const u8, bytes_read as usize);
                    let vec = slice.to_vec();
                    ffi::sal_free_read_buffer(buffer_ptr);
                    vec
                } else {
                    Vec::new()
                };

                // Get ETag if available
                let e_tag = if !meta.etag.is_null() {
                    Some(CStr::from_ptr(meta.etag).to_string_lossy().to_string())
                } else {
                    None
                };

                let obj_meta = ObjectMeta {
                    location: location.clone(),
                    last_modified: DateTime::from_timestamp(meta.mtime, 0).unwrap_or_default(),
                    size: meta.size as usize,
                    e_tag,
                    version: None,
                };

                ffi::sal_destroy_object(object);
                ffi::sal_destroy_bucket(bucket);

                Ok(GetResult {
                    payload: GetResultPayload::Stream(
                        futures::stream::once(async move { Ok(Bytes::from(buffer)) }).boxed()
                    ),
                    meta: obj_meta,
                    range: opts.range,
                    attributes: Default::default(),
                })
            }
        })
        .await
        .map_err(|e| Error::Generic {
            store: "SAL",
            source: Box::new(e),
        })?
    }

    /// Delete objects
    pub async fn delete_stream<'a>(
        &self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        let conn = match self.get_connection() {
            Ok(c) => c,
            Err(e) => {
                return futures::stream::once(async move { Err(e) }).boxed();
            }
        };

        locations
            .then(move |location_result| {
                let conn = Arc::clone(&conn);
                async move {
                    let location = location_result?;
                    let loc = location.clone();

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

                            // Get object
                            let key_cstr = CString::new(loc.as_ref()).map_err(|e| Error::Generic {
                                store: "SAL",
                                source: Box::new(e),
                            })?;

                            let object = ffi::sal_get_object(bucket, ptr::null_mut(), key_cstr.as_ptr());

                            if object.is_null() {
                                // Object doesn't exist - that's OK for delete
                                ffi::sal_destroy_bucket(bucket);
                                return Ok(location);
                            }

                            // Delete object
                            let ret = ffi::sal_delete_object(object, ptr::null_mut());

                            ffi::sal_destroy_object(object);
                            ffi::sal_destroy_bucket(bucket);

                            // -ENOENT is not an error for delete
                            if ret < 0 && ret != ffi::errors::ENOENT {
                                return Err(map_sal_error(ret, "sal_delete_object"));
                            }

                            Ok(location)
                        }
                    })
                    .await
                    .map_err(|e| Error::Generic {
                        store: "SAL",
                        source: Box::new(e),
                    })?
                }
            })
            .boxed()
    }

    /// List objects with optional prefix
    pub async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'static, Result<ObjectMeta>>> {
        let prefix_str = prefix.map(|p| p.as_ref().to_string());
        let conn = self.get_connection()?;

        Ok(futures::stream::once(async move {
            task::spawn_blocking(move || {
                let mut results = Vec::new();

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

                    // List objects
                    let prefix_cstr = prefix_str.as_ref().map(|s| {
                        CString::new(s.as_str()).map_err(|e| Error::Generic {
                            store: "SAL",
                            source: Box::new(e),
                        })
                    }).transpose()?;

                    let mut keys_ptr: *mut *mut c_char = ptr::null_mut();
                    let mut count: i32 = 0;

                    let ret = ffi::sal_list_objects(
                        bucket,
                        ptr::null_mut(),
                        prefix_cstr.as_ref().map_or(ptr::null(), |s| s.as_ptr()),
                        ptr::null(),
                        1000,
                        ptr::null(),
                        &mut keys_ptr,
                        &mut count,
                    );

                    if ret < 0 {
                        ffi::sal_destroy_bucket(bucket);
                        return Err(map_sal_error(ret, "sal_list_objects"));
                    }

                    // Process results
                    if !keys_ptr.is_null() && count > 0 {
                        let keys_slice = std::slice::from_raw_parts(keys_ptr, count as usize);

                        for key_ptr in keys_slice {
                            if key_ptr.is_null() {
                                continue;
                            }

                            let key = CStr::from_ptr(*key_ptr).to_string_lossy().to_string();

                            // Get object metadata
                            let key_cstr = CString::new(key.as_str()).unwrap();
                            let object = ffi::sal_get_object(bucket, ptr::null_mut(), key_cstr.as_ptr());

                            if !object.is_null() {
                                let mut meta: ffi::SalObjectMeta = std::mem::zeroed();
                                let meta_ret = ffi::sal_get_object_meta(object, ptr::null_mut(), &mut meta);

                                if meta_ret >= 0 {
                                    let e_tag = if !meta.etag.is_null() {
                                        Some(CStr::from_ptr(meta.etag).to_string_lossy().to_string())
                                    } else {
                                        None
                                    };

                                    results.push(Ok(ObjectMeta {
                                        location: Path::from(key),
                                        last_modified: DateTime::from_timestamp(meta.mtime, 0).unwrap_or_default(),
                                        size: meta.size as usize,
                                        e_tag,
                                        version: None,
                                    }));
                                }

                                ffi::sal_destroy_object(object);
                            }
                        }

                        ffi::sal_free_object_list(keys_ptr, count);
                    }

                    ffi::sal_destroy_bucket(bucket);
                }

                Ok(futures::stream::iter(results))
            })
            .await
            .map_err(|e| Error::Generic {
                store: "SAL",
                source: Box::new(e),
            })?
        })
        .flatten()
        .boxed())
    }

    /// Start a multipart upload
    pub async fn put_multipart(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let upload = SalMultipartUpload::new(self.clone(), location.clone())?;
        Ok(Box::new(upload))
    }
}

/// Map SAL error codes to object_store errors
fn map_sal_error(code: i32, context: &str) -> Error {
    match code {
        ffi::errors::ENOENT => Error::NotFound {
            path: context.to_string(),
            source: format!("SAL error: {}", code).into(),
        },
        ffi::errors::EEXIST => Error::AlreadyExists {
            path: context.to_string(),
            source: format!("SAL error: {}", code).into(),
        },
        ffi::errors::EACCES => Error::Generic {
            store: "SAL",
            source: format!("Permission denied: {}", context).into(),
        },
        _ => Error::Generic {
            store: "SAL",
            source: format!("{} failed with error code: {}", context, code).into(),
        },
    }
}
