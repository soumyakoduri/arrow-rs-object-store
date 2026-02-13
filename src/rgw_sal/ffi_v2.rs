/**
 * Minimal Rust FFI Layer for SAL Unified API
 *
 * TRUE 1-1 MAPPING: Each ObjectStore method = ONE extern "C" function
 * NO business logic, NO conversions, just extern declarations
 */

use std::os::raw::{c_char, c_int};

/* ========================================================================
 * Opaque Types (Minimal - Only Context and Driver)
 * ======================================================================== */

#[repr(C)]
pub struct SalContext {
    _private: [u8; 0],
}

#[repr(C)]
pub struct SalDriver {
    _private: [u8; 0],
}

/* ========================================================================
 * Data Structures
 * ======================================================================== */

#[repr(C)]
pub struct SalObjectMeta {
    pub size: u64,
    pub mtime_sec: i64,
    pub mtime_nsec: i64,
    pub etag: *mut c_char,
}

#[repr(C)]
pub struct SalListEntry {
    pub key: *mut c_char,
    pub size: u64,
    pub mtime_sec: i64,
    pub mtime_nsec: i64,
    pub etag: *mut c_char,
}

#[repr(C)]
pub struct SalListResult {
    pub entries: *mut SalListEntry,
    pub count: usize,
    pub common_prefixes: *mut *mut c_char,
    pub prefix_count: usize,
    pub next_marker: *mut c_char,
}

#[repr(C)]
pub struct SalByteRange {
    pub start: u64,
    pub end: u64,
}

#[repr(C)]
pub struct SalRangeData {
    pub data: *mut c_char,
    pub len: u64,
    pub range: SalByteRange,
}

#[repr(C)]
pub struct SalDeleteResult {
    pub key: *mut c_char,
    pub error_code: c_int,
}

#[repr(C)]
pub struct SalConditionals {
    pub if_match: *const c_char,
    pub if_none_match: *const c_char,
    pub if_modified_since: i64,
    pub if_unmodified_since: i64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub enum SalPutMode {
    Create = 1,
    Overwrite = 2,
    Update = 3,
}

/* ========================================================================
 * FFI Declarations - TRUE 1-1 MAPPING
 * ======================================================================== */

#[link(name = "sal_unified", kind = "static")]
extern "C" {
    /* Initialization */
    pub fn sal_ctx_create(
        cluster: *const c_char,
        user: *const c_char,
        conf: *const c_char,
    ) -> *mut SalContext;

    pub fn sal_ctx_destroy(ctx: *mut SalContext);

    pub fn sal_driver_create_rados(ctx: *mut SalContext) -> *mut SalDriver;

    pub fn sal_driver_destroy(driver: *mut SalDriver);

    /* TRUE 1-1 MAPPED APIs */

    /// PUT object - ObjectStore::put_opts
    pub fn sal_put_object(
        driver: *mut SalDriver,
        bucket_name: *const c_char,
        key: *const c_char,
        data: *const c_char,
        len: u64,
        mode: SalPutMode,
        etag: *mut *mut c_char,
    ) -> c_int;

    /// GET object - ObjectStore::get_opts
    pub fn sal_get_object(
        driver: *mut SalDriver,
        bucket_name: *const c_char,
        key: *const c_char,
        offset: u64,
        len: u64,
        conds: *mut SalConditionals,
        buffer: *mut *mut c_char,
        bytes_read: *mut u64,
        meta: *mut SalObjectMeta,
    ) -> c_int;

    /// DELETE object - ObjectStore::delete
    pub fn sal_delete_object(
        driver: *mut SalDriver,
        bucket_name: *const c_char,
        key: *const c_char,
    ) -> c_int;

    /// LIST objects - ObjectStore::list
    pub fn sal_list_objects(
        driver: *mut SalDriver,
        bucket_name: *const c_char,
        prefix: *const c_char,
        marker: *const c_char,
        max_keys: c_int,
        result: *mut SalListResult,
    ) -> c_int;

    /// LIST objects with delimiter - ObjectStore::list_with_delimiter
    pub fn sal_list_objects_with_delimiter(
        driver: *mut SalDriver,
        bucket_name: *const c_char,
        prefix: *const c_char,
        delimiter: *const c_char,
        marker: *const c_char,
        max_keys: c_int,
        result: *mut SalListResult,
    ) -> c_int;

    /// COPY object - ObjectStore::copy_opts
    pub fn sal_copy_object(
        driver: *mut SalDriver,
        src_bucket: *const c_char,
        src_key: *const c_char,
        dst_bucket: *const c_char,
        dst_key: *const c_char,
    ) -> c_int;

    /// DELETE multiple objects - ObjectStore::delete_stream
    pub fn sal_delete_objects(
        driver: *mut SalDriver,
        bucket_name: *const c_char,
        keys: *const *const c_char,
        count: usize,
        results: *mut *mut SalDeleteResult,
        result_count: *mut usize,
    ) -> c_int;

    /// GET object ranges - ObjectStore::get_ranges
    pub fn sal_get_object_ranges(
        driver: *mut SalDriver,
        bucket_name: *const c_char,
        key: *const c_char,
        ranges: *mut SalByteRange,
        range_count: usize,
        results: *mut *mut SalRangeData,
        result_count: *mut usize,
    ) -> c_int;

    /// INIT multipart - ObjectStore::put_multipart_opts
    pub fn sal_init_multipart(
        driver: *mut SalDriver,
        bucket_name: *const c_char,
        key: *const c_char,
        upload_id: *mut *mut c_char,
    ) -> c_int;

    /// PUT multipart part - MultipartUpload::put_part
    pub fn sal_multipart_put_part(
        driver: *mut SalDriver,
        bucket_name: *const c_char,
        key: *const c_char,
        upload_id: *const c_char,
        part_num: c_int,
        data: *const c_char,
        len: u64,
        etag: *mut *mut c_char,
    ) -> c_int;

    /// COMPLETE multipart - MultipartUpload::complete
    pub fn sal_multipart_complete(
        driver: *mut SalDriver,
        bucket_name: *const c_char,
        key: *const c_char,
        upload_id: *const c_char,
        part_etags: *const *const c_char,
        num_parts: c_int,
        final_etag: *mut *mut c_char,
    ) -> c_int;

    /// ABORT multipart - MultipartUpload::abort
    pub fn sal_multipart_abort(
        driver: *mut SalDriver,
        bucket_name: *const c_char,
        key: *const c_char,
        upload_id: *const c_char,
    ) -> c_int;

    /* Memory Management */
    pub fn sal_free_string(s: *mut c_char);
    pub fn sal_free_buffer(buf: *mut c_char);
    pub fn sal_object_meta_free(meta: *mut SalObjectMeta);
    pub fn sal_list_result_free(result: *mut SalListResult);
    pub fn sal_range_data_free(results: *mut SalRangeData, count: usize);
    pub fn sal_delete_results_free(results: *mut SalDeleteResult, count: usize);
}

/* ========================================================================
 * Single Helper Function - Error Mapping
 * ======================================================================== */

/// Convert SAL error code (negative errno) to object_store::Error
pub fn sal_error_to_object_store(code: c_int, path: &str) -> object_store::Error {
    use object_store::Error;
    use std::io;

    let io_err = io::Error::from_raw_os_error(-code);

    match code {
        -2 => Error::NotFound {
            path: path.to_string(),
            source: Box::new(io_err),
        },
        -17 => Error::AlreadyExists {
            path: path.to_string(),
            source: Box::new(io_err),
        },
        _ => Error::Generic {
            store: "SAL",
            source: Box::new(io_err),
        },
    }
}

/* ========================================================================
 * Safety Markers
 * ======================================================================== */

unsafe impl Send for SalContext {}
unsafe impl Send for SalDriver {}

unsafe impl Sync for SalContext {}
unsafe impl Sync for SalDriver {}
