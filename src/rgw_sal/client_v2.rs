/**
 * Simplified SAL Client Implementation (v2)
 *
 * TRUE 1-1 MAPPING: Each ObjectStore method calls exactly ONE C function.
 * All logic is in the C layer - this is just a thin async wrapper.
 */

use super::ffi_v2 as ffi;
use crate::{
    path::Path, Attributes, Error, GetOptions, GetResult, GetResultPayload, ListResult,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
    list::{PaginatedListStore, PaginatedListOptions, PaginatedListResult},
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use futures::stream::{self, BoxStream, StreamExt};
use std::ffi::{CStr, CString};
use std::fmt;
use std::ops::Range;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;
use tokio::task;

/* ========================================================================
 * Resource Handle Wrappers (RAII)
 * ======================================================================== */

struct SalContextHandle(*mut ffi::SalContext);

impl Drop for SalContextHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::sal_ctx_destroy(self.0);
        }
    }
}

unsafe impl Send for SalContextHandle {}
unsafe impl Sync for SalContextHandle {}

struct SalDriverHandle(*mut ffi::SalDriver);

impl Drop for SalDriverHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::sal_driver_destroy(self.0);
        }
    }
}

unsafe impl Send for SalDriverHandle {}
unsafe impl Sync for SalDriverHandle {}

/* ========================================================================
 * SAL Client
 * ======================================================================== */

#[derive(Debug)]
pub struct SalClient {
    ctx: Arc<SalContextHandle>,
    driver: Arc<SalDriverHandle>,
    bucket_name: String,
}

impl SalClient {
    pub fn new(
        cluster_name: String,
        user_name: String,
        conf_file: Option<String>,
        bucket_name: String,
    ) -> Result<Self> {
        let cluster_cstr = CString::new(cluster_name)?;
        let user_cstr = CString::new(user_name)?;
        let conf_cstr = conf_file.map(CString::new).transpose()?;

        unsafe {
            let ctx = ffi::sal_ctx_create(
                cluster_cstr.as_ptr(),
                user_cstr.as_ptr(),
                conf_cstr.as_ref().map_or(ptr::null(), |c| c.as_ptr()),
            );

            if ctx.is_null() {
                return Err(Error::Generic {
                    store: "SAL",
                    source: "Failed to create SAL context".into(),
                });
            }

            let driver = ffi::sal_driver_create_rados(ctx);
            if driver.is_null() {
                ffi::sal_ctx_destroy(ctx);
                return Err(Error::Generic {
                    store: "SAL",
                    source: "Failed to create RADOS driver".into(),
                });
            }

            Ok(Self {
                ctx: Arc::new(SalContextHandle(ctx)),
                driver: Arc::new(SalDriverHandle(driver)),
                bucket_name,
            })
        }
    }

    unsafe fn cstr_to_string(ptr: *mut c_char) -> Option<String> {
        if ptr.is_null() {
            None
        } else {
            let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
            ffi::sal_free_string(ptr);
            Some(s)
        }
    }
}

impl fmt::Display for SalClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SalClient(bucket={})", self.bucket_name)
    }
}

#[async_trait]
impl ObjectStore for SalClient {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let data: Bytes = payload.into();
        let key = CString::new(location.as_ref())?;
        let bucket_name = CString::new(self.bucket_name.as_str())?;
        let driver = self.driver.0;
        let location_str = location.as_ref().to_string();

        task::spawn_blocking(move || unsafe {
            let mode = match opts.mode {
                PutMode::Create => ffi::SalPutMode::Create,
                PutMode::Overwrite => ffi::SalPutMode::Overwrite,
                PutMode::Update(_) => ffi::SalPutMode::Update,
            };

            let mut etag_ptr = ptr::null_mut();

            // ONE FUNCTION CALL - all logic in C
            let ret = ffi::sal_put_object(
                driver,
                bucket_name.as_ptr(),
                key.as_ptr(),
                data.as_ptr() as *const i8,
                data.len() as u64,
                mode,
                &mut etag_ptr,
            );

            if ret < 0 {
                return Err(ffi::sal_error_to_object_store(ret, &location_str));
            }

            Ok(PutResult {
                e_tag: Self::cstr_to_string(etag_ptr),
                version: None,
            })
        })
        .await?
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let key = CString::new(location.as_ref())?;
        let bucket_name = CString::new(self.bucket_name.as_str())?;
        let driver = self.driver.0;
        let location_str = location.as_ref().to_string();

        task::spawn_blocking(move || unsafe {
            // Setup conditionals
            let mut conds = ffi::SalConditionals {
                if_match: ptr::null(),
                if_none_match: ptr::null(),
                if_modified_since: 0,
                if_unmodified_since: 0,
            };

            let if_match_cstr;
            let if_none_match_cstr;

            if let Some(ref etag) = options.if_match {
                if_match_cstr = CString::new(etag.as_str()).ok();
                if let Some(ref s) = if_match_cstr {
                    conds.if_match = s.as_ptr();
                }
            }

            if let Some(ref etag) = options.if_none_match {
                if_none_match_cstr = CString::new(etag.as_str()).ok();
                if let Some(ref s) = if_none_match_cstr {
                    conds.if_none_match = s.as_ptr();
                }
            }

            if let Some(dt) = options.if_modified_since {
                conds.if_modified_since = dt.timestamp();
            }

            if let Some(dt) = options.if_unmodified_since {
                conds.if_unmodified_since = dt.timestamp();
            }

            let (offset, length) = if let Some(ref range) = options.range {
                (range.start, range.end - range.start)
            } else {
                (0, 0)
            };

            let mut buffer = ptr::null_mut();
            let mut bytes_read = 0u64;
            let mut meta = ffi::SalObjectMeta {
                size: 0,
                mtime_sec: 0,
                mtime_nsec: 0,
                etag: ptr::null_mut(),
            };

            // ONE FUNCTION CALL - all logic in C
            let ret = ffi::sal_get_object(
                driver,
                bucket_name.as_ptr(),
                key.as_ptr(),
                offset,
                length,
                &mut conds,
                &mut buffer,
                &mut bytes_read,
                &mut meta,
            );

            if ret < 0 {
                return Err(ffi::sal_error_to_object_store(ret, &location_str));
            }

            // Convert to Rust types
            let e_tag = Self::cstr_to_string(meta.etag);
            let last_modified = Utc
                .timestamp_opt(meta.mtime_sec, meta.mtime_nsec as u32)
                .single()
                .unwrap_or_else(|| Utc::now());

            let data = if bytes_read > 0 && !buffer.is_null() {
                let slice = std::slice::from_raw_parts(buffer as *const u8, bytes_read as usize);
                let bytes = Bytes::copy_from_slice(slice);
                ffi::sal_free_buffer(buffer);
                bytes
            } else {
                Bytes::new()
            };

            let object_meta = ObjectMeta {
                location: Path::from(location_str.clone()),
                last_modified,
                size: meta.size as usize,
                e_tag,
                version: None,
            };

            let range = Range {
                start: offset,
                end: offset + bytes_read,
            };

            Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(stream::once(async move { Ok(data) }))),
                meta: object_meta,
                range,
                attributes: Attributes::default(),
            })
        })
        .await?
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let key = CString::new(location.as_ref())?;
        let bucket_name = CString::new(self.bucket_name.as_str())?;
        let driver = self.driver.0;
        let location_str = location.as_ref().to_string();

        task::spawn_blocking(move || unsafe {
            // ONE FUNCTION CALL - all logic in C
            let ret = ffi::sal_delete_object(driver, bucket_name.as_ptr(), key.as_ptr());

            if ret < 0 {
                return Err(ffi::sal_error_to_object_store(ret, &location_str));
            }

            Ok(())
        })
        .await?
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'static, Result<ObjectMeta>>> {
        self.list_with_delimiter(prefix).await.map(|list_result| {
            stream::iter(list_result.objects.into_iter().map(Ok)).boxed()
        })
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix_str = prefix.map(|p| p.as_ref().to_string());
        let bucket_name = CString::new(self.bucket_name.as_str())?;
        let driver = self.driver.0;

        task::spawn_blocking(move || unsafe {
            let prefix_cstr = prefix_str
                .as_ref()
                .map(|s| CString::new(s.as_str()))
                .transpose()?;

            let mut all_objects = Vec::new();
            let mut marker: Option<CString> = None;

            // Loop through all pages
            loop {
                let mut result = ffi::SalListResult {
                    entries: ptr::null_mut(),
                    count: 0,
                    common_prefixes: ptr::null_mut(),
                    prefix_count: 0,
                    next_marker: ptr::null_mut(),
                };

                let ret = ffi::sal_list_objects(
                    driver,
                    bucket_name.as_ptr(),
                    prefix_cstr.as_ref().map_or(ptr::null(), |s| s.as_ptr()),
                    marker.as_ref().map_or(ptr::null(), |m| m.as_ptr()),
                    1000,
                    &mut result,
                );

                if ret < 0 {
                    return Err(ffi::sal_error_to_object_store(ret, "list"));
                }

                // Convert entries from this page
                if result.count > 0 && !result.entries.is_null() {
                    let entries = std::slice::from_raw_parts(result.entries, result.count);

                    for entry in entries {
                        let key = CStr::from_ptr(entry.key).to_string_lossy().into_owned();
                        let e_tag = if !entry.etag.is_null() {
                            Some(CStr::from_ptr(entry.etag).to_string_lossy().into_owned())
                        } else {
                            None
                        };

                        let last_modified = Utc
                            .timestamp_opt(entry.mtime_sec, entry.mtime_nsec as u32)
                            .single()
                            .unwrap_or_else(|| Utc::now());

                        all_objects.push(ObjectMeta {
                            location: Path::from(key),
                            last_modified,
                            size: entry.size as usize,
                            e_tag,
                            version: None,
                        });
                    }
                }

                // Check if there are more pages
                let has_more = !result.next_marker.is_null();
                if has_more {
                    // Save the marker for next iteration
                    let marker_str = CStr::from_ptr(result.next_marker)
                        .to_string_lossy()
                        .into_owned();
                    marker = Some(CString::new(marker_str).unwrap());
                }

                ffi::sal_list_result_free(&mut result);

                // Break if no more pages
                if !has_more {
                    break;
                }
            }

            Ok(ListResult {
                common_prefixes: vec![],
                objects: all_objects,
            })
        })
        .await?
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn crate::MultipartUpload>> {
        let key = CString::new(location.as_ref())?;
        let bucket_name = CString::new(self.bucket_name.as_str())?;
        let driver = self.driver.0;
        let location_str = location.as_ref().to_string();
        let bucket_clone = self.bucket_name.clone();

        let upload_id = task::spawn_blocking(move || unsafe {
            let mut upload_id_ptr = ptr::null_mut();

            // ONE FUNCTION CALL - all logic in C
            let ret = ffi::sal_init_multipart(
                driver,
                bucket_name.as_ptr(),
                key.as_ptr(),
                &mut upload_id_ptr,
            );

            if ret < 0 {
                return Err(ffi::sal_error_to_object_store(ret, &location_str));
            }

            Ok(SalClient::cstr_to_string(upload_id_ptr).unwrap_or_default())
        })
        .await??;

        Ok(Box::new(SalMultipartUpload {
            driver: Arc::clone(&self.driver),
            bucket_name: bucket_clone,
            key: location.as_ref().to_string(),
            upload_id,
            part_num: 1,
        }))
    }
}

/* ========================================================================
 * Multipart Upload
 * ======================================================================== */

struct SalMultipartUpload {
    driver: Arc<SalDriverHandle>,
    bucket_name: String,
    key: String,
    upload_id: String,
    part_num: i32,
}

#[async_trait]
impl crate::MultipartUpload for SalMultipartUpload {
    async fn put_part(&mut self, data: PutPayload) -> Result<crate::UploadPart> {
        let bytes: Bytes = data.into();
        let driver = self.driver.0;
        let bucket_name = CString::new(self.bucket_name.as_str())?;
        let key = CString::new(self.key.as_str())?;
        let upload_id = CString::new(self.upload_id.as_str())?;
        let part_num = self.part_num;
        self.part_num += 1;

        let etag = task::spawn_blocking(move || unsafe {
            let mut etag_ptr = ptr::null_mut();

            // ONE FUNCTION CALL - all logic in C
            let ret = ffi::sal_multipart_put_part(
                driver,
                bucket_name.as_ptr(),
                key.as_ptr(),
                upload_id.as_ptr(),
                part_num,
                bytes.as_ptr() as *const i8,
                bytes.len() as u64,
                &mut etag_ptr,
            );

            if ret < 0 {
                return Err(ffi::sal_error_to_object_store(ret, "multipart_put_part"));
            }

            Ok(SalClient::cstr_to_string(etag_ptr).unwrap_or_default())
        })
        .await??;

        Ok(crate::UploadPart { content_id: etag })
    }

    async fn complete(&mut self, parts: Vec<crate::UploadPart>) -> Result<PutResult> {
        let driver = self.driver.0;
        let bucket_name = CString::new(self.bucket_name.as_str())?;
        let key = CString::new(self.key.as_str())?;
        let upload_id = CString::new(self.upload_id.as_str())?;

        // Convert part ETags to C strings
        let part_etag_cstrs: Vec<CString> = parts
            .iter()
            .map(|p| CString::new(p.content_id.as_str()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let part_etag_ptrs: Vec<*const i8> = part_etag_cstrs.iter().map(|s| s.as_ptr()).collect();

        let etag = task::spawn_blocking(move || unsafe {
            let mut final_etag_ptr = ptr::null_mut();

            // ONE FUNCTION CALL - all logic in C
            let ret = ffi::sal_multipart_complete(
                driver,
                bucket_name.as_ptr(),
                key.as_ptr(),
                upload_id.as_ptr(),
                part_etag_ptrs.as_ptr(),
                part_etag_ptrs.len() as i32,
                &mut final_etag_ptr,
            );

            if ret < 0 {
                return Err(ffi::sal_error_to_object_store(ret, "multipart_complete"));
            }

            Ok(SalClient::cstr_to_string(final_etag_ptr))
        })
        .await??;

        Ok(PutResult {
            e_tag: etag,
            version: None,
        })
    }

    async fn abort(&mut self) -> Result<()> {
        let driver = self.driver.0;
        let bucket_name = CString::new(self.bucket_name.as_str())?;
        let key = CString::new(self.key.as_str())?;
        let upload_id = CString::new(self.upload_id.as_str())?;

        task::spawn_blocking(move || unsafe {
            // ONE FUNCTION CALL - all logic in C
            let ret = ffi::sal_multipart_abort(
                driver,
                bucket_name.as_ptr(),
                key.as_ptr(),
                upload_id.as_ptr(),
            );

            if ret < 0 {
                return Err(ffi::sal_error_to_object_store(ret, "multipart_abort"));
            }

            Ok(())
        })
        .await?
    }
}

impl fmt::Debug for SalMultipartUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SalMultipartUpload")
            .field("upload_id", &self.upload_id)
            .field("key", &self.key)
            .field("part_num", &self.part_num)
            .finish()
    }
}

/* ========================================================================
 * Paginated List Support
 * ======================================================================== */

#[async_trait]
impl PaginatedListStore for SalClient {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        opts: PaginatedListOptions,
    ) -> Result<PaginatedListResult> {
        let prefix_str = prefix.map(|p| p.to_string());
        let bucket_name = CString::new(self.bucket_name.as_str())?;
        let driver = self.driver.0;
        let max_keys = opts.max_keys.unwrap_or(1000).min(1000) as i32;
        let marker = opts.page_token.clone();

        task::spawn_blocking(move || unsafe {
            let prefix_cstr = prefix_str
                .as_ref()
                .map(|s| CString::new(s.as_str()))
                .transpose()?;

            let marker_cstr = marker
                .as_ref()
                .map(|s| CString::new(s.as_str()))
                .transpose()?;

            let mut result = ffi::SalListResult {
                entries: ptr::null_mut(),
                count: 0,
                common_prefixes: ptr::null_mut(),
                prefix_count: 0,
                next_marker: ptr::null_mut(),
            };

            let ret = ffi::sal_list_objects(
                driver,
                bucket_name.as_ptr(),
                prefix_cstr.as_ref().map_or(ptr::null(), |s| s.as_ptr()),
                marker_cstr.as_ref().map_or(ptr::null(), |s| s.as_ptr()),
                max_keys,
                &mut result,
            );

            if ret < 0 {
                return Err(ffi::sal_error_to_object_store(ret, "list_paginated"));
            }

            // Convert entries
            let mut objects = Vec::new();
            if result.count > 0 && !result.entries.is_null() {
                let entries = std::slice::from_raw_parts(result.entries, result.count);

                for entry in entries {
                    let key = CStr::from_ptr(entry.key).to_string_lossy().into_owned();
                    let e_tag = if !entry.etag.is_null() {
                        Some(CStr::from_ptr(entry.etag).to_string_lossy().into_owned())
                    } else {
                        None
                    };

                    let last_modified = Utc
                        .timestamp_opt(entry.mtime_sec, entry.mtime_nsec as u32)
                        .single()
                        .unwrap_or_else(|| Utc::now());

                    objects.push(ObjectMeta {
                        location: Path::from(key),
                        last_modified,
                        size: entry.size as usize,
                        e_tag,
                        version: None,
                    });
                }
            }

            // Get next page token
            let page_token = if !result.next_marker.is_null() {
                Some(CStr::from_ptr(result.next_marker).to_string_lossy().into_owned())
            } else {
                None
            };

            ffi::sal_list_result_free(&mut result);

            Ok(PaginatedListResult {
                result: ListResult {
                    common_prefixes: vec![],
                    objects,
                },
                page_token,
            })
        })
        .await?
    }
}
