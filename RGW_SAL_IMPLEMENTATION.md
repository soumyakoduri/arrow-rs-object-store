# RGW SAL Backend Implementation Guide

## Overview

This document outlines the implementation of a Ceph RGW (RADOS Gateway) backend for `object_store` using the **SAL (Storage Abstraction Layer)** APIs instead of raw RADOS.

## Why SAL Instead of RADOS?

### RADOS Limitations

Raw RADOS is a low-level object storage API that lacks object store semantics:

| Feature | Raw RADOS | RGW SAL |
|---------|-----------|---------|
| **Object Store API** | ❌ No buckets/objects | ✅ S3-like buckets/objects |
| **Multipart Upload** | ❌ Must implement manually | ✅ Native support |
| **Object Metadata** | ❌ Manual xattr management | ✅ Built-in metadata |
| **Versioning** | ❌ Not supported | ✅ Object versioning |
| **Access Control** | ❌ Manual ACLs | ✅ S3-compatible ACLs |
| **Lifecycle** | ❌ Not supported | ✅ Object lifecycle rules |
| **Listing** | ❌ Flat namespace | ✅ Bucket listing with prefixes |

### SAL Advantages

**SAL is the abstraction layer used by RGW itself**, providing:

1. **Object Store Semantics**: Buckets, objects, metadata - maps naturally to `ObjectStore` trait
2. **Battle-Tested**: Powers RGW's S3/Swift API implementations
3. **Feature-Rich**: Multipart uploads, versioning, ACLs out of the box
4. **Extensible**: Multiple storage backends (RADOS, DBStore, POSIX, etc.)

## Architecture

### SAL Layer Hierarchy

```
┌─────────────────────────────────────────────────────────────┐
│                   object_store Crate                         │
│                  (Rust ObjectStore trait)                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  RGW SAL Rust Wrapper                        │
│              (Our implementation: rgw_sal.rs)                │
│  - RgwSalBuilder                                            │
│  - RgwSalClient                                             │
│  - RgwSalMultipartUpload                                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼ (FFI via cxx or bindgen)
┌─────────────────────────────────────────────────────────────┐
│              RGW SAL C++ API (rgw_sal.h)                     │
│                                                              │
│  Driver                                                      │
│  ├─> User                                                    │
│  ├─> Bucket                                                  │
│  │   ├─> list_objects()                                      │
│  │   └─> get_object()                                        │
│  └─> Object                                                  │
│      ├─> get_data()                                          │
│      ├─> set_obj_attrs()                                     │
│      └─> Writer (for put operations)                         │
│          └─> MultipartUpload                                 │
│              ├─> upload_part()                               │
│              ├─> complete()                                  │
│              └─> abort()                                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Storage Backend                           │
│  - RADOSStore (RADOS)                                       │
│  - DBStore (RocksDB/SQLite)                                 │
│  - POSIXDriver (POSIX filesystem)                           │
└─────────────────────────────────────────────────────────────┘
```

### Key SAL Components

From `rgw_sal.h`, the main interfaces are:

#### 1. **Driver** (formerly called Store)

The top-level abstraction representing a storage backend.

```cpp
class Driver {
public:
  virtual std::unique_ptr<User> get_user(const rgw_user& u) = 0;
  virtual int get_bucket(User* user, const RGWBucketInfo& info,
                         std::unique_ptr<Bucket>* bucket) = 0;
  virtual std::unique_ptr<Writer> get_writer(User* user, Bucket* bucket,
                                              const rgw_obj& obj) = 0;
  // ... many more methods
};
```

**Maps to**: `ObjectStore` instance in Rust

#### 2. **Bucket**

Represents an S3-style bucket.

```cpp
class Bucket {
public:
  virtual int list(const DoutPrefixProvider* dpp,
                   ListParams& params,
                   int max,
                   ListResults& results) = 0;

  virtual int remove_object(const DoutPrefixProvider* dpp,
                             const rgw_obj_key& key) = 0;

  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) = 0;

  // Bucket metadata
  virtual RGWBucketInfo& get_info() = 0;
  virtual Attrs& get_attrs() = 0;
};
```

**Maps to**: Collection of objects under a prefix in `ObjectStore`

#### 3. **Object**

Represents a single object within a bucket.

```cpp
class Object {
public:
  virtual int get_obj_state(const DoutPrefixProvider* dpp,
                             RGWObjState** state) = 0;

  virtual int read(int64_t offset, int64_t length, bufferlist& bl) = 0;

  virtual int copy_object(const DoutPrefixProvider* dpp,
                           User* user,
                           Bucket* dest_bucket,
                           Object* dest_object) = 0;

  virtual std::unique_ptr<Writer> get_writer(Bucket* bucket) = 0;
};
```

**Maps to**: Individual file/object in `ObjectStore`

#### 4. **Writer** (for PUT operations)

Handles object writes, including multipart uploads.

```cpp
class Writer {
public:
  virtual int prepare() = 0;

  virtual int process(bufferlist&& data, uint64_t offset) = 0;

  virtual int complete(size_t accounted_size,
                        const std::string& etag,
                        ceph::real_time* mtime,
                        ceph::real_time set_mtime) = 0;

  // Multipart upload support
  virtual MultipartUpload* get_multipart_upload() = 0;
};
```

**Maps to**: `PutPayload` and `MultipartUpload` in Rust

#### 5. **MultipartUpload**

Handles multipart uploads (similar to S3 multipart).

```cpp
class MultipartUpload {
public:
  virtual int init(const DoutPrefixProvider* dpp,
                    ACLOwner& owner,
                    rgw_placement_rule& dest_placement) = 0;

  virtual int upload_part(const DoutPrefixProvider* dpp,
                           int part_num,
                           bufferlist& data,
                           uint64_t size) = 0;

  virtual int complete(const DoutPrefixProvider* dpp,
                        std::map<int, std::string>& part_etags,
                        RGWObjManifest*& manifest) = 0;

  virtual int abort(const DoutPrefixProvider* dpp) = 0;
};
```

**Maps to**: `MultipartUpload` trait in Rust

## Implementation Plan

### Phase 1: FFI Bindings

Create Rust FFI bindings to SAL C++ APIs using `cxx` crate.

**File**: `src/rgw-sal/sal_bindings.rs`

```rust
#[cxx::bridge(namespace = "rgw::sal")]
mod ffi {
    unsafe extern "C++" {
        include!("rgw/rgw_sal.h");

        // Driver interface
        type Driver;

        fn create_rados_driver(
            cct: *mut c_void,
            enable_gc_threads: bool,
            enable_lc_threads: bool,
            enable_quota_threads: bool,
            enable_sync_threads: bool,
            run_sync_thread: bool,
        ) -> UniquePtr<Driver>;

        // Bucket interface
        type Bucket;

        fn get_bucket(
            self: &Driver,
            user: *const User,
            bucket_name: &CxxString,
        ) -> Result<UniquePtr<Bucket>>;

        fn list_objects(
            self: &Bucket,
            prefix: &CxxString,
            delimiter: &CxxString,
            max_keys: u32,
        ) -> Result<Vec<ObjectMetadata>>;

        // Object interface
        type Object;
        type ObjectMetadata;

        fn get_object(
            self: &Bucket,
            key: &CxxString,
        ) -> Result<UniquePtr<Object>>;

        fn read_object(
            self: &Object,
            offset: i64,
            length: i64,
        ) -> Result<Vec<u8>>;

        // Writer interface
        type Writer;

        fn get_writer(
            self: &Driver,
            bucket: *const Bucket,
            key: &CxxString,
        ) -> UniquePtr<Writer>;

        fn write_data(
            self: Pin<&mut Writer>,
            data: &[u8],
            offset: u64,
        ) -> Result<()>;

        fn complete_write(
            self: Pin<&mut Writer>,
        ) -> Result<String>;  // Returns ETag

        // Multipart upload interface
        type MultipartUpload;

        fn init_multipart(
            self: Pin<&mut Writer>,
            upload_id: &CxxString,
        ) -> Result<UniquePtr<MultipartUpload>>;

        fn upload_part(
            self: Pin<&mut MultipartUpload>,
            part_num: i32,
            data: &[u8],
        ) -> Result<String>;  // Returns part ETag

        fn complete_multipart(
            self: Pin<&mut MultipartUpload>,
            part_etags: &CxxVector<PartETag>,
        ) -> Result<String>;

        fn abort_multipart(
            self: Pin<&mut MultipartUpload>,
        ) -> Result<()>;
    }
}
```

### Phase 2: Rust Wrapper Layer

Implement safe Rust wrappers around SAL FFI.

**File**: `src/rgw-sal/client.rs`

```rust
use crate::rgw_sal::ffi;
use crate::{Error, Result, Path, PutPayload, PutOptions, GetOptions, GetResult};

pub struct RgwSalClient {
    driver: cxx::UniquePtr<ffi::Driver>,
    bucket_name: String,
}

impl RgwSalClient {
    pub fn new(
        cluster_name: String,
        user_name: String,
        conf_file: Option<String>,
        bucket_name: String,
        keyring: Option<String>,
        key: Option<String>,
    ) -> Result<Self> {
        // 1. Initialize Ceph context
        let cct = unsafe {
            // Call ceph::common::CephContext::create()
            // Configure with cluster_name, user_name, conf_file, keyring/key
            initialize_ceph_context(&cluster_name, &user_name, conf_file.as_deref())?
        };

        // 2. Create SAL driver (RADOSStore backend)
        let driver = ffi::create_rados_driver(
            cct,
            true,  // enable_gc_threads
            true,  // enable_lc_threads
            true,  // enable_quota_threads
            false, // enable_sync_threads
            false, // run_sync_thread
        )?;

        Ok(Self {
            driver,
            bucket_name,
        })
    }

    pub async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        // 1. Get bucket
        let bucket = self.driver.get_bucket(
            std::ptr::null(),  // User pointer (can be null for admin)
            &self.bucket_name,
        )?;

        // 2. Get writer
        let mut writer = self.driver.get_writer(
            &bucket,
            location.as_ref(),
        )?;

        // 3. Prepare write
        writer.pin_mut().prepare()?;

        // 4. Collect payload bytes
        let data: Vec<u8> = payload.iter().flatten().copied().collect();

        // 5. Write data
        writer.pin_mut().write_data(&data, 0)?;

        // 6. Complete write
        let etag = writer.pin_mut().complete_write()?;

        Ok(PutResult {
            e_tag: Some(etag),
            version: None,
        })
    }

    pub async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult> {
        // 1. Get bucket
        let bucket = self.driver.get_bucket(
            std::ptr::null(),
            &self.bucket_name,
        )?;

        // 2. Get object
        let object = bucket.get_object(location.as_ref())?;

        // 3. Read data (handle range if specified)
        let (offset, length) = if let Some(range) = options.range {
            (range.start as i64, (range.end - range.start) as i64)
        } else {
            (0, -1)  // Read entire object
        };

        let data = object.read_object(offset, length)?;

        // 4. Get object metadata
        let state = object.get_obj_state()?;

        Ok(GetResult {
            payload: GetResultPayload::Stream(
                futures::stream::once(async move { Ok(Bytes::from(data)) }).boxed()
            ),
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: state.mtime,
                size: state.size as usize,
                e_tag: state.etag,
                version: None,
            },
            range: offset as usize..(offset + data.len() as i64) as usize,
            attributes: Default::default(),
        })
    }

    pub fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let bucket_name = self.bucket_name.clone();
        let prefix_str = prefix.map(|p| p.as_ref().to_string()).unwrap_or_default();

        // Get bucket
        let bucket = match self.driver.get_bucket(std::ptr::null(), &bucket_name) {
            Ok(b) => b,
            Err(e) => return futures::stream::once(async { Err(e) }).boxed(),
        };

        // List objects
        let objects = match bucket.list_objects(&prefix_str, "", 1000) {
            Ok(objs) => objs,
            Err(e) => return futures::stream::once(async { Err(e) }).boxed(),
        };

        // Convert to ObjectMeta stream
        futures::stream::iter(objects.into_iter().map(|obj| {
            Ok(ObjectMeta {
                location: Path::from(obj.key),
                last_modified: obj.mtime,
                size: obj.size as usize,
                e_tag: Some(obj.etag),
                version: None,
            })
        }))
        .boxed()
    }
}
```

### Phase 3: Multipart Upload Implementation

**File**: `src/rgw-sal/multipart.rs`

```rust
use crate::rgw_sal::ffi;
use crate::{MultipartUpload, PutPayload, PutResult, Result};

pub struct RgwSalMultipartUpload {
    upload: cxx::UniquePtr<ffi::MultipartUpload>,
    upload_id: String,
    parts: Vec<(usize, String)>,  // (part_number, etag)
}

#[async_trait]
impl MultipartUpload for RgwSalMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let part_num = self.parts.len() + 1;
        let mut upload = self.upload.clone();

        Box::pin(async move {
            // Collect part data
            let bytes: Vec<u8> = data.iter().flatten().copied().collect();

            // Upload part to SAL
            let etag = upload.pin_mut().upload_part(part_num as i32, &bytes)?;

            // Store part metadata
            self.parts.push((part_num, etag));

            Ok(())
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        // Build part ETags map
        let part_etags: Vec<ffi::PartETag> = self.parts
            .iter()
            .map(|(num, etag)| ffi::PartETag {
                part_number: *num as i32,
                etag: etag.clone(),
            })
            .collect();

        // Complete multipart upload via SAL
        let final_etag = self.upload.pin_mut().complete_multipart(&part_etags)?;

        Ok(PutResult {
            e_tag: Some(final_etag),
            version: None,
        })
    }

    async fn abort(&mut self) -> Result<()> {
        self.upload.pin_mut().abort_multipart()?;
        Ok(())
    }
}
```

### Phase 4: Integration with object_store

Update `src/lib.rs`:

```rust
#[cfg(feature = "rgw-sal")]
pub mod rgw_sal;

#[cfg_attr(
    feature = "rgw-sal",
    doc = "* [`rgw_sal`]: [Ceph RGW via SAL](https://docs.ceph.com/en/latest/radosgw/). See [`RgwSalBuilder`](rgw_sal::RgwSalBuilder)"
)]
```

Update `Cargo.toml`:

```toml
[dependencies]
# Ceph RGW SAL support
cxx = { version = "1.0", optional = true }

[build-dependencies]
cxx-build = { version = "1.0", optional = true }

[features]
rgw-sal = ["cxx", "cxx-build"]
```

Create `build.rs` for C++ compilation:

```rust
#[cfg(feature = "rgw-sal")]
fn build_sal_bindings() {
    cxx_build::bridge("src/rgw-sal/sal_bindings.rs")
        .file("src/rgw-sal/sal_wrapper.cpp")
        .flag_if_supported("-std=c++17")
        .include("/usr/include/ceph")
        .compile("rgw_sal_bindings");

    println!("cargo:rustc-link-lib=rados");
    println!("cargo:rustc-link-lib=rgw");
    println!("cargo:rerun-if-changed=src/rgw-sal/sal_bindings.rs");
    println!("cargo:rerun-if-changed=src/rgw-sal/sal_wrapper.cpp");
}

fn main() {
    #[cfg(feature = "rgw-sal")]
    build_sal_bindings();
}
```

## Implementation Challenges

### 1. **C++ FFI Complexity**

SAL is a pure C++ API with no C bindings. Solutions:

- **Option A**: Use `cxx` crate for safe C++/Rust interop
- **Option B**: Write C wrapper layer, then use `bindgen`
- **Option C**: Use `autocxx` for automatic binding generation

**Recommendation**: Use `cxx` for type-safe, zero-overhead FFI.

### 2. **Async Bridge**

SAL uses synchronous C++ APIs, but `ObjectStore` is async.

**Solution**: Wrap blocking SAL calls in `tokio::task::spawn_blocking()`:

```rust
pub async fn get_object(&self, location: &Path) -> Result<GetResult> {
    let driver = self.driver.clone();
    let bucket_name = self.bucket_name.clone();
    let location = location.clone();

    tokio::task::spawn_blocking(move || {
        // Blocking SAL calls here
        let bucket = driver.get_bucket(&bucket_name)?;
        let object = bucket.get_object(location.as_ref())?;
        let data = object.read_object(0, -1)?;
        Ok(GetResult { /* ... */ })
    })
    .await?
}
```

### 3. **Lifetime and Ownership**

SAL uses raw pointers and manual memory management.

**Solution**: Use `cxx::UniquePtr<T>` for automatic cleanup:

```rust
struct RgwSalClient {
    driver: cxx::UniquePtr<ffi::Driver>,  // Automatically destroyed
    // ...
}
```

### 4. **Error Handling**

SAL returns C++ integers for errors (`int ret`).

**Solution**: Convert to Rust `Result<T>`:

```rust
fn check_sal_error(ret: i32, operation: &str) -> Result<()> {
    if ret < 0 {
        Err(Error::Generic {
            store: "RGW-SAL",
            source: format!("{} failed with code {}", operation, ret).into(),
        })
    } else {
        Ok(())
    }
}
```

## Testing Strategy

### Unit Tests

Test Rust wrapper layer without real Ceph cluster:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder() {
        let builder = RgwSalBuilder::new()
            .with_bucket("test-bucket")
            .with_cluster_name("ceph");

        assert!(builder.build().is_ok());
    }

    // Mock SAL driver for testing
    #[test]
    fn test_put_get_roundtrip() {
        // Use mock SAL driver
        // Test put → get → verify data
    }
}
```

### Integration Tests

Test against real Ceph cluster:

```bash
# Start local Ceph cluster (vstart.sh)
cd /path/to/ceph/build
../src/vstart.sh -d -n -x

# Set environment variables
export CEPH_BUCKET_NAME=test-bucket
export CEPH_CONF=ceph.conf

# Run tests
cargo test --features rgw-sal --test integration_tests
```

## Comparison: SAL vs RADOS

### Multipart Upload

**RADOS (our previous implementation)**:
```
User uploads 3 parts:
  → Create temp objects: uuid-part-00000, uuid-part-00001, uuid-part-00002
  → On complete: concatenate parts using rados_append()
  → Delete temp objects

Drawbacks:
  - 2x storage overhead during upload
  - Manual concatenation (I/O overhead)
  - Custom implementation (potential bugs)
```

**SAL (new implementation)**:
```
User uploads 3 parts:
  → upload_part(1, data) → SAL handles storage
  → upload_part(2, data) → SAL handles storage
  → upload_part(3, data) → SAL handles storage
  → complete() → SAL assembles final object

Advantages:
  - Native multipart (proven implementation)
  - Efficient storage (SAL-optimized)
  - Same code path as S3/Swift multipart
```

## Next Steps

1. **Create FFI bindings** using `cxx` crate
2. **Implement RgwSalClient** wrapper around SAL Driver
3. **Implement multipart upload** using SAL's native multipart
4. **Add integration tests** against real Ceph cluster
5. **Optimize performance** (connection pooling, async I/O)
6. **Documentation** and examples

## Resources

- **RGW SAL Header**: https://github.com/ceph/ceph/blob/main/src/rgw/rgw_sal.h
- **RGW SAL Documentation**: https://docs.ceph.com/en/latest/radosgw/
- **cxx Crate**: https://cxx.rs/
- **Ceph Developer Guide**: https://docs.ceph.com/en/latest/dev/

## Conclusion

Using RGW SAL instead of raw RADOS provides:
- ✅ Native object store semantics (buckets, objects, metadata)
- ✅ Built-in multipart upload (no custom implementation)
- ✅ Battle-tested code path (powers RGW's S3/Swift APIs)
- ✅ Easier to maintain (less custom code)

The main challenge is C++ FFI, but `cxx` crate provides a safe, zero-cost solution.
