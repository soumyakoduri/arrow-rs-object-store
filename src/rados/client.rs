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

//! RADOS client implementation using librados FFI bindings

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

use super::ffi::{self, RadosIoCtxT, RadosListCtxT, RadosT};
use super::multipart::RadosMultipartUpload;

/// RADOS connection handle
///
/// This wraps the librados cluster and I/O context handles.
/// It's designed to be thread-safe and can be cloned cheaply.
#[derive(Debug)]
pub(crate) struct RadosConnection {
    pub(crate) cluster: *mut RadosT,
    pub(crate) ioctx: *mut RadosIoCtxT,
}

// Safety: We ensure thread-safety by only accessing these pointers
// from blocking tasks and properly synchronizing access
unsafe impl Send for RadosConnection {}
unsafe impl Sync for RadosConnection {}

impl Drop for RadosConnection {
    fn drop(&mut self) {
        unsafe {
            if !self.ioctx.is_null() {
                ffi::rados_ioctx_destroy(self.ioctx);
            }
            if !self.cluster.is_null() {
                ffi::rados_shutdown(self.cluster);
            }
        }
    }
}

/// Client for interacting with Ceph RADOS
#[derive(Debug, Clone)]
pub struct RadosClient {
    cluster_name: String,
    user_name: String,
    conf_file: Option<String>,
    pool_name: String,
    namespace: Option<String>,
    keyring: Option<String>,
    key: Option<String>,
    // Connection is created on-demand and cached
    connection: Arc<parking_lot::Mutex<Option<Arc<RadosConnection>>>>,
}

impl RadosClient {
    /// Create a new RadosClient
    pub fn new(
        cluster_name: String,
        user_name: String,
        conf_file: Option<String>,
        pool_name: String,
        namespace: Option<String>,
        keyring: Option<String>,
        key: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            cluster_name,
            user_name,
            conf_file,
            pool_name,
            namespace,
            keyring,
            key,
            connection: Arc::new(parking_lot::Mutex::new(None)),
        })
    }

    /// Get the pool name
    pub fn pool_name(&self) -> &str {
        &self.pool_name
    }

    /// Get or create a RADOS connection
    pub(crate) fn get_connection(&self) -> Result<Arc<RadosConnection>> {
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

    /// Create a new RADOS connection
    fn create_connection(&self) -> Result<RadosConnection> {
        unsafe {
            let mut cluster: *mut RadosT = ptr::null_mut();
            let mut ioctx: *mut RadosIoCtxT = ptr::null_mut();

            // Create cluster handle
            let user_id = CString::new(self.user_name.as_str()).map_err(|e| Error::Generic {
                store: "RADOS",
                source: Box::new(e),
            })?;

            let ret = ffi::rados_create(&mut cluster, user_id.as_ptr());
            if ret < 0 {
                return Err(map_rados_error(ret, "rados_create"));
            }

            // Read config file if provided
            if let Some(ref conf_file) = self.conf_file {
                let conf_path = CString::new(conf_file.as_str()).map_err(|e| Error::Generic {
                    store: "RADOS",
                    source: Box::new(e),
                })?;

                let ret = ffi::rados_conf_read_file(cluster, conf_path.as_ptr());
                if ret < 0 {
                    ffi::rados_shutdown(cluster);
                    return Err(map_rados_error(ret, "rados_conf_read_file"));
                }
            }

            // Configure keyring or key if provided
            if let Some(ref keyring) = self.keyring {
                let key_name = CString::new("keyring").unwrap();
                let key_value = CString::new(keyring.as_str()).map_err(|e| Error::Generic {
                    store: "RADOS",
                    source: Box::new(e),
                })?;

                let ret = ffi::rados_conf_set(cluster, key_name.as_ptr(), key_value.as_ptr());
                if ret < 0 {
                    ffi::rados_shutdown(cluster);
                    return Err(map_rados_error(ret, "rados_conf_set (keyring)"));
                }
            } else if let Some(ref key) = self.key {
                let key_name = CString::new("key").unwrap();
                let key_value = CString::new(key.as_str()).map_err(|e| Error::Generic {
                    store: "RADOS",
                    source: Box::new(e),
                })?;

                let ret = ffi::rados_conf_set(cluster, key_name.as_ptr(), key_value.as_ptr());
                if ret < 0 {
                    ffi::rados_shutdown(cluster);
                    return Err(map_rados_error(ret, "rados_conf_set (key)"));
                }
            }

            // Connect to cluster
            let ret = ffi::rados_connect(cluster);
            if ret < 0 {
                ffi::rados_shutdown(cluster);
                return Err(map_rados_error(ret, "rados_connect"));
            }

            // Create I/O context for the pool
            let pool_cstr = CString::new(self.pool_name.as_str()).map_err(|e| Error::Generic {
                store: "RADOS",
                source: Box::new(e),
            })?;

            let ret = ffi::rados_ioctx_create(cluster, pool_cstr.as_ptr(), &mut ioctx);
            if ret < 0 {
                ffi::rados_shutdown(cluster);
                return Err(map_rados_error(ret, "rados_ioctx_create"));
            }

            // Set namespace if provided
            if let Some(ref namespace) = self.namespace {
                let ns_cstr = CString::new(namespace.as_str()).map_err(|e| Error::Generic {
                    store: "RADOS",
                    source: Box::new(e),
                })?;

                ffi::rados_ioctx_set_namespace(ioctx, ns_cstr.as_ptr());
            }

            Ok(RadosConnection { cluster, ioctx })
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
            let oid = CString::new(location.as_ref()).map_err(|e| Error::Generic {
                store: "RADOS",
                source: Box::new(e),
            })?;

            unsafe {
                // Check if object exists for PutMode handling
                if matches!(opts.mode, PutMode::Create) {
                    let mut size: u64 = 0;
                    let ret = ffi::rados_stat(conn.ioctx, oid.as_ptr(), &mut size, ptr::null_mut());
                    if ret >= 0 {
                        // Object exists
                        return Err(Error::AlreadyExists {
                            path: location.to_string(),
                            source: "Object already exists".into(),
                        });
                    }
                }

                // Write the object
                let ret = ffi::rados_write_full(
                    conn.ioctx,
                    oid.as_ptr(),
                    data.as_ptr() as *const c_char,
                    data.len(),
                );

                if ret < 0 {
                    return Err(map_rados_error(ret, "rados_write_full"));
                }

                // Set e_tag as xattr if provided
                if let Some(ref e_tag) = opts.attributes.e_tag {
                    let attr_name = CString::new("user.e_tag").unwrap();
                    let attr_value = CString::new(e_tag.as_str()).map_err(|e| Error::Generic {
                        store: "RADOS",
                        source: Box::new(e),
                    })?;

                    ffi::rados_setxattr(
                        conn.ioctx,
                        oid.as_ptr(),
                        attr_name.as_ptr(),
                        attr_value.as_ptr() as *const c_char,
                        attr_value.as_bytes().len(),
                    );
                }

                Ok(PutResult {
                    e_tag: opts.attributes.e_tag,
                    version: None,
                })
            }
        })
        .await
        .map_err(|e| Error::Generic {
            store: "RADOS",
            source: Box::new(e),
        })?
    }

    /// Get an object with options
    pub async fn get_opts(&self, location: &Path, opts: GetOptions) -> Result<GetResult> {
        let location = location.clone();
        let conn = self.get_connection()?;

        task::spawn_blocking(move || {
            let oid = CString::new(location.as_ref()).map_err(|e| Error::Generic {
                store: "RADOS",
                source: Box::new(e),
            })?;

            unsafe {
                // Get object size and mtime
                let mut size: u64 = 0;
                let mut mtime: i64 = 0;
                let ret = ffi::rados_stat(conn.ioctx, oid.as_ptr(), &mut size, &mut mtime);

                if ret < 0 {
                    return Err(map_rados_error(ret, "rados_stat"));
                }

                // Handle range if specified
                let (offset, read_len) = if let Some(range) = opts.range {
                    let start = range.start.unwrap_or(0) as u64;
                    let end = range.end.map(|e| e as u64).unwrap_or(size);
                    (start, (end - start) as usize)
                } else {
                    (0, size as usize)
                };

                // Read the data
                let mut buffer = vec![0u8; read_len];
                let ret = ffi::rados_read(
                    conn.ioctx,
                    oid.as_ptr(),
                    buffer.as_mut_ptr() as *mut c_char,
                    read_len,
                    offset,
                );

                if ret < 0 {
                    return Err(map_rados_error(ret, "rados_read"));
                }

                let bytes_read = ret as usize;
                buffer.truncate(bytes_read);

                // Try to get e_tag from xattr
                let mut etag_buf = vec![0u8; 256];
                let attr_name = CString::new("user.e_tag").unwrap();
                let etag_ret = ffi::rados_getxattr(
                    conn.ioctx,
                    oid.as_ptr(),
                    attr_name.as_ptr(),
                    etag_buf.as_mut_ptr() as *mut c_char,
                    etag_buf.len(),
                );

                let e_tag = if etag_ret > 0 {
                    etag_buf.truncate(etag_ret as usize);
                    String::from_utf8(etag_buf).ok()
                } else {
                    None
                };

                let meta = ObjectMeta {
                    location: location.clone(),
                    last_modified: DateTime::from_timestamp(mtime, 0).unwrap_or_default(),
                    size: size as usize,
                    e_tag,
                    version: None,
                };

                Ok(GetResult {
                    payload: GetResultPayload::Stream(
                        futures::stream::once(async move { Ok(Bytes::from(buffer)) }).boxed()
                    ),
                    meta,
                    range: opts.range,
                    attributes: Default::default(),
                })
            }
        })
        .await
        .map_err(|e| Error::Generic {
            store: "RADOS",
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
                        let oid = CString::new(loc.as_ref()).map_err(|e| Error::Generic {
                            store: "RADOS",
                            source: Box::new(e),
                        })?;

                        unsafe {
                            let ret = ffi::rados_remove(conn.ioctx, oid.as_ptr());
                            // -ENOENT is not an error for delete
                            if ret < 0 && ret != ffi::errors::ENOENT {
                                return Err(map_rados_error(ret, "rados_remove"));
                            }
                        }

                        Ok(location)
                    })
                    .await
                    .map_err(|e| Error::Generic {
                        store: "RADOS",
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
        let location_prefix = prefix.map(|p| p.clone());

        Ok(futures::stream::once(async move {
            task::spawn_blocking(move || {
                let mut results = Vec::new();

                unsafe {
                    let mut ctx: *mut RadosListCtxT = ptr::null_mut();
                    let ret = ffi::rados_nobjects_list_open(conn.ioctx, &mut ctx);

                    if ret < 0 {
                        return Err(map_rados_error(ret, "rados_nobjects_list_open"));
                    }

                    loop {
                        let mut entry: *const c_char = ptr::null();
                        let ret = ffi::rados_nobjects_list_next(
                            ctx,
                            &mut entry,
                            ptr::null_mut(),
                            ptr::null_mut(),
                        );

                        if ret < 0 {
                            ffi::rados_nobjects_list_close(ctx);
                            return Err(map_rados_error(ret, "rados_nobjects_list_next"));
                        }

                        if entry.is_null() {
                            break;
                        }

                        let name = CStr::from_ptr(entry).to_string_lossy().to_string();

                        // Filter by prefix if specified
                        if let Some(ref prefix) = prefix_str {
                            if !name.starts_with(prefix) {
                                continue;
                            }
                        }

                        // Get object metadata
                        let oid = CString::new(name.as_str()).unwrap();
                        let mut size: u64 = 0;
                        let mut mtime: i64 = 0;
                        let stat_ret = ffi::rados_stat(conn.ioctx, oid.as_ptr(), &mut size, &mut mtime);

                        if stat_ret >= 0 {
                            let location = Path::from(name);
                            results.push(Ok(ObjectMeta {
                                location,
                                last_modified: DateTime::from_timestamp(mtime, 0).unwrap_or_default(),
                                size: size as usize,
                                e_tag: None,
                                version: None,
                            }));
                        }
                    }

                    ffi::rados_nobjects_list_close(ctx);
                }

                Ok(futures::stream::iter(results))
            })
            .await
            .map_err(|e| Error::Generic {
                store: "RADOS",
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
        let upload = RadosMultipartUpload::new(self.clone(), location.clone())?;
        Ok(Box::new(upload))
    }
}

/// Map RADOS error codes to object_store errors
fn map_rados_error(code: i32, context: &str) -> Error {
    match code {
        ffi::errors::ENOENT => Error::NotFound {
            path: context.to_string(),
            source: format!("RADOS error: {}", code).into(),
        },
        ffi::errors::EEXIST => Error::AlreadyExists {
            path: context.to_string(),
            source: format!("RADOS error: {}", code).into(),
        },
        ffi::errors::EACCES => Error::Generic {
            store: "RADOS",
            source: format!("Permission denied: {}", context).into(),
        },
        _ => Error::Generic {
            store: "RADOS",
            source: format!("{} failed with error code: {}", context, code).into(),
        },
    }
}
