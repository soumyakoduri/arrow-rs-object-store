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

//! SAL Client Implementation
//!
//! This module provides the core implementation of the SAL ObjectStore client.

use super::ffi;
use crate::{
    path::Path, Error, GetOptions, GetResult, GetResultPayload, ListResult, ObjectMeta,
    ObjectStore, PutMode, PutOptions, PutPayload, PutResult, Result,
};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream, StreamExt};
use parking_lot::Mutex;
use std::ffi::{CStr, CString};
use std::fmt;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;
use tokio::task;

/// SAL connection state
struct SalConnection {
    cct: *mut ffi::CephContext,
    driver: *mut ffi::SalDriver,
    bucket_name: String,
}

impl SalConnection {
    /// Create a new SAL connection
    unsafe fn new(
        cluster_name: &str,
        user_name: &str,
        conf_file: Option<&str>,
        bucket_name: &str,
    ) -> Result<Self> {
        // Create CephContext
        let cluster_cstr = CString::new(cluster_name)
            .map_err(|e| Error::Generic {
                store: "SAL",
                source: Box::new(e),
            })?;
        let user_cstr = CString::new(user_name)
            .map_err(|e| Error::Generic {
                store: "SAL",
                source: Box::new(e),
            })?;
        let conf_cstr = conf_file
            .map(|s| CString::new(s))
            .transpose()
            .map_err(|e| Error::Generic {
                store: "SAL",
                source: Box::new(e),
            })?;

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

        // Create SAL Driver
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
            bucket_name: bucket_name.to_string(),
        })
    }
}

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

// Ensure SalConnection can be sent between threads
unsafe impl Send for SalConnection {}
unsafe impl Sync for SalConnection {}

/// SAL Client
pub struct SalClient {
    cluster_name: String,
    user_name: String,
    conf_file: Option<String>,
    bucket_name: String,
    // Lazy connection (created on first use, cached for reuse)
    connection: Arc<Mutex<Option<Arc<SalConnection>>>>,
}

impl SalClient {
    /// Create a new SAL client
    pub fn new(
        cluster_name: String,
        user_name: String,
        conf_file: Option<String>,
        bucket_name: String,
    ) -> Self {
        Self {
            cluster_name,
            user_name,
            conf_file,
            bucket_name,
            connection: Arc::new(Mutex::new(None)),
        }
    }

    /// Get or create the SAL connection
    pub(crate) fn get_connection(&self) -> Result<Arc<SalConnection>> {
        let mut conn_guard = self.connection.lock();

        if let Some(conn) = conn_guard.as_ref() {
            return Ok(Arc::clone(conn));
        }

        // Create new connection
        let conn = unsafe {
            SalConnection::new(
                &self.cluster_name,
                &self.user_name,
                self.conf_file.as_deref(),
                &self.bucket_name,
            )?
        };

        let arc_conn = Arc::new(conn);
        *conn_guard = Some(Arc::clone(&arc_conn));

        Ok(arc_conn)
    }

    /// Implement put operation
    pub async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        // Collect payload into a single buffer
        let data: Bytes = payload
            .iter()
            .fold(Bytes::new(), |mut acc, chunk| {
                let mut new_buf = BytesMut::with_capacity(acc.len() + chunk.len());
                new_buf.extend_from_slice(&acc);
                new_buf.extend_from_slice(chunk);
                new_buf.freeze()
            });

        let location = location.clone();
        let conn = self.get_connection()?;

        // Run in blocking task
        task::spawn_blocking(move || {
            unsafe {
                // 1. Get bucket
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

                // 2. Handle PutMode::Create - check if object exists
                let key_cstr = CString::new(location.as_ref())
                    .map_err(|e| Error::Generic {
                        store: "SAL",
                        source: Box::new(e),
                    })?;

                if matches!(opts.mode, PutMode::Create) {
                    let obj = ffi::sal_get_object(
                        bucket,
                        ptr::null_mut(),
                        key_cstr.as_ptr(),
                    );
                    if !obj.is_null() {
                        let mut meta = std::mem::zeroed::<ffi::SalObjectMeta>();
                        let meta_ret = ffi::sal_get_object_meta(
                            obj,
                            ptr::null_mut(),
                            &mut meta,
                        );
                        ffi::sal_destroy_object(obj);

                        if meta_ret >= 0 {
                            ffi::sal_destroy_bucket(bucket);
                            return Err(Error::AlreadyExists {
                                path: location.to_string(),
                                source: "Object already exists".into(),
                            });
                        }
                    }
                }

                // 3. Create writer
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

                // 4. Prepare
                let ret = ffi::sal_writer_prepare(writer, ptr::null_mut());
                if ret < 0 {
                    ffi::sal_destroy_writer(writer);
                    ffi::sal_destroy_bucket(bucket);
                    return Err(ffi::map_sal_error(ret, "sal_writer_prepare"));
                }

                // 5. Write data
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
                    return Err(ffi::map_sal_error(ret, "sal_writer_write"));
                }

                // 6. Complete
                let mut etag_ptr: *mut c_char = ptr::null_mut();
                let ret = ffi::sal_writer_complete(
                    writer,
                    ptr::null_mut(),
                    &mut etag_ptr,
                );

                ffi::sal_destroy_writer(writer);
                ffi::sal_destroy_bucket(bucket);

                if ret < 0 {
                    return Err(ffi::map_sal_error(ret, "sal_writer_complete"));
                }

                let e_tag = if !etag_ptr.is_null() {
                    let etag_str = CStr::from_ptr(etag_ptr)
                        .to_string_lossy()
                        .to_string();
                    ffi::sal_free_etag(etag_ptr);
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

    /// Implement get operation
    pub async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult> {
        let location = location.clone();
        let conn = self.get_connection()?;

        task::spawn_blocking(move || {
            unsafe {
                // 1. Get bucket
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

                // 2. Get object
                let key_cstr = CString::new(location.as_ref())
                    .map_err(|e| Error::Generic {
                        store: "SAL",
                        source: Box::new(e),
                    })?;
                let object = ffi::sal_get_object(
                    bucket,
                    ptr::null_mut(),
                    key_cstr.as_ptr(),
                );

                if object.is_null() {
                    ffi::sal_destroy_bucket(bucket);
                    return Err(Error::NotFound {
                        path: location.to_string(),
                        source: "Object not found".into(),
                    });
                }

                // 3. Get metadata
                let mut meta: ffi::SalObjectMeta = std::mem::zeroed();
                let ret = ffi::sal_get_object_meta(
                    object,
                    ptr::null_mut(),
                    &mut meta,
                );

                if ret < 0 {
                    ffi::sal_destroy_object(object);
                    ffi::sal_destroy_bucket(bucket);
                    return Err(ffi::map_sal_error(ret, "sal_get_object_meta"));
                }

                // 4. Handle range
                let (offset, read_len) = if let Some(range) = options.range {
                    let start = range.start.unwrap_or(0) as u64;
                    let end = range.end.map(|e| e as u64).unwrap_or(meta.size);
                    (start, end.saturating_sub(start))
                } else {
                    (0, meta.size)
                };

                // 5. Handle head-only request
                if options.head {
                    let e_tag = if !meta.etag.is_null() {
                        Some(CStr::from_ptr(meta.etag)
                            .to_string_lossy()
                            .to_string())
                    } else {
                        None
                    };

                    let obj_meta = ObjectMeta {
                        location: location.clone(),
                        last_modified: DateTime::from_timestamp(meta.mtime, 0)
                            .unwrap_or_else(|| Utc::now()),
                        size: meta.size as usize,
                        e_tag,
                        version: None,
                    };

                    ffi::sal_destroy_object(object);
                    ffi::sal_destroy_bucket(bucket);

                    return Ok(GetResult {
                        payload: GetResultPayload::Stream(
                            stream::empty().boxed(),
                        ),
                        meta: obj_meta,
                        range: options.range,
                        attributes: Default::default(),
                    });
                }

                // 6. Read data
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
                    return Err(ffi::map_sal_error(ret, "sal_read_object"));
                }

                // 7. Copy to Rust Vec
                let buffer = if !buffer_ptr.is_null() && bytes_read > 0 {
                    let slice = std::slice::from_raw_parts(
                        buffer_ptr as *const u8,
                        bytes_read as usize,
                    );
                    let vec = slice.to_vec();
                    ffi::sal_free_read_buffer(buffer_ptr);
                    vec
                } else {
                    Vec::new()
                };

                // 8. Extract metadata
                let e_tag = if !meta.etag.is_null() {
                    Some(CStr::from_ptr(meta.etag)
                        .to_string_lossy()
                        .to_string())
                } else {
                    None
                };

                let obj_meta = ObjectMeta {
                    location: location.clone(),
                    last_modified: DateTime::from_timestamp(meta.mtime, 0)
                        .unwrap_or_else(|| Utc::now()),
                    size: meta.size as usize,
                    e_tag,
                    version: None,
                };

                ffi::sal_destroy_object(object);
                ffi::sal_destroy_bucket(bucket);

                Ok(GetResult {
                    payload: GetResultPayload::Stream(
                        stream::once(async move { Ok(Bytes::from(buffer)) })
                            .boxed(),
                    ),
                    meta: obj_meta,
                    range: options.range,
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

    /// Delete an object
    pub async fn delete(&self, location: &Path) -> Result<()> {
        let location = location.clone();
        let conn = self.get_connection()?;

        task::spawn_blocking(move || {
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

                let key_cstr = CString::new(location.as_ref())
                    .map_err(|e| Error::Generic {
                        store: "SAL",
                        source: Box::new(e),
                    })?;
                let object = ffi::sal_get_object(
                    bucket,
                    ptr::null_mut(),
                    key_cstr.as_ptr(),
                );

                if object.is_null() {
                    ffi::sal_destroy_bucket(bucket);
                    // Not found is OK for delete
                    return Ok(());
                }

                let ret = ffi::sal_delete_object(object, ptr::null_mut());

                ffi::sal_destroy_object(object);
                ffi::sal_destroy_bucket(bucket);

                if ret < 0 {
                    return Err(ffi::map_sal_error(ret, "sal_delete_object"));
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

    /// List objects in bucket
    pub fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix_str = prefix.map(|p| p.to_string());
        let conn = match self.get_connection() {
            Ok(c) => c,
            Err(e) => return stream::once(async move { Err(e) }).boxed(),
        };

        let stream = stream::unfold(
            (conn, prefix_str, String::new(), false),
            |(conn, prefix, marker, done)| async move {
                if done {
                    return None;
                }

                let result = task::spawn_blocking(move || {
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

                        let prefix_cstr = prefix
                            .as_ref()
                            .map(|s| CString::new(s.as_str()))
                            .transpose()
                            .map_err(|e| Error::Generic {
                                store: "SAL",
                                source: Box::new(e),
                            })?;

                        let marker_cstr = if !marker.is_empty() {
                            Some(CString::new(marker.as_str()).map_err(|e| Error::Generic {
                                store: "SAL",
                                source: Box::new(e),
                            })?)
                        } else {
                            None
                        };

                        let mut keys_ptr: *mut *mut c_char = ptr::null_mut();
                        let mut count: i32 = 0;

                        let ret = ffi::sal_list_objects(
                            bucket,
                            ptr::null_mut(),
                            prefix_cstr.as_ref().map_or(ptr::null(), |s| s.as_ptr()),
                            ptr::null(), // No delimiter (recursive)
                            1000,        // Max keys per request
                            marker_cstr.as_ref().map_or(ptr::null(), |s| s.as_ptr()),
                            &mut keys_ptr,
                            &mut count,
                        );

                        ffi::sal_destroy_bucket(bucket);

                        if ret < 0 {
                            return Err(ffi::map_sal_error(ret, "sal_list_objects"));
                        }

                        // Extract keys
                        let mut objects = Vec::new();
                        let mut new_marker = String::new();

                        if !keys_ptr.is_null() && count > 0 {
                            let keys_slice = std::slice::from_raw_parts(keys_ptr, count as usize);
                            for &key_ptr in keys_slice {
                                if !key_ptr.is_null() {
                                    let key_str = CStr::from_ptr(key_ptr)
                                        .to_string_lossy()
                                        .to_string();
                                    new_marker = key_str.clone();

                                    // Get object metadata for size/mtime
                                    objects.push(ObjectMeta {
                                        location: Path::from(key_str.as_str()),
                                        last_modified: Utc::now(), // Would need separate call for exact time
                                        size: 0,                   // Would need separate call for exact size
                                        e_tag: None,
                                        version: None,
                                    });
                                }
                            }
                            ffi::sal_free_list_result(keys_ptr, count);
                        }

                        let is_done = count < 1000;

                        Ok((objects, new_marker, is_done))
                    }
                })
                .await;

                match result {
                    Ok(Ok((objects, new_marker, is_done))) => {
                        Some((
                            stream::iter(objects.into_iter().map(Ok)),
                            (conn, prefix, new_marker, is_done),
                        ))
                    }
                    Ok(Err(e)) => Some((
                        stream::once(async move { Err(e) }),
                        (conn, prefix, marker, true),
                    )),
                    Err(e) => Some((
                        stream::once(async move {
                            Err(Error::Generic {
                                store: "SAL",
                                source: Box::new(e),
                            })
                        }),
                        (conn, prefix, marker, true),
                    )),
                }
            },
        )
        .flatten();

        stream.boxed()
    }

    /// List objects with delimiter (hierarchical)
    pub async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix_str = prefix.map(|p| p.to_string());
        let conn = self.get_connection()?;

        task::spawn_blocking(move || {
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

                let prefix_cstr = prefix_str
                    .as_ref()
                    .map(|s| CString::new(s.as_str()))
                    .transpose()
                    .map_err(|e| Error::Generic {
                        store: "SAL",
                        source: Box::new(e),
                    })?;

                let delimiter_cstr = CString::new("/").unwrap();

                let mut keys_ptr: *mut *mut c_char = ptr::null_mut();
                let mut count: i32 = 0;

                let ret = ffi::sal_list_objects(
                    bucket,
                    ptr::null_mut(),
                    prefix_cstr.as_ref().map_or(ptr::null(), |s| s.as_ptr()),
                    delimiter_cstr.as_ptr(),
                    1000,
                    ptr::null(),
                    &mut keys_ptr,
                    &mut count,
                );

                ffi::sal_destroy_bucket(bucket);

                if ret < 0 {
                    return Err(ffi::map_sal_error(ret, "sal_list_objects"));
                }

                // TODO: Properly parse common prefixes vs objects
                // For now, return everything as objects
                let mut objects = Vec::new();

                if !keys_ptr.is_null() && count > 0 {
                    let keys_slice = std::slice::from_raw_parts(keys_ptr, count as usize);
                    for &key_ptr in keys_slice {
                        if !key_ptr.is_null() {
                            let key_str = CStr::from_ptr(key_ptr)
                                .to_string_lossy()
                                .to_string();

                            objects.push(ObjectMeta {
                                location: Path::from(key_str.as_str()),
                                last_modified: Utc::now(),
                                size: 0,
                                e_tag: None,
                                version: None,
                            });
                        }
                    }
                    ffi::sal_free_list_result(keys_ptr, count);
                }

                Ok(ListResult {
                    common_prefixes: Vec::new(), // TODO: Parse from delimiter
                    objects,
                })
            }
        })
        .await
        .map_err(|e| Error::Generic {
            store: "SAL",
            source: Box::new(e),
        })?
    }
}

impl fmt::Debug for SalClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SalClient")
            .field("cluster_name", &self.cluster_name)
            .field("user_name", &self.user_name)
            .field("bucket_name", &self.bucket_name)
            .finish()
    }
}

impl fmt::Display for SalClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SalObjectStore(cluster={}, bucket={})",
            self.cluster_name, self.bucket_name
        )
    }
}
