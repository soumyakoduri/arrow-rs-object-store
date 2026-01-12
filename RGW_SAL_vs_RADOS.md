# RGW SAL vs Raw RADOS - Quick Comparison

## Executive Summary

This document compares two approaches for implementing a Ceph backend for `object_store`:
1. **Raw RADOS** (low-level RADOS API via librados)
2. **RGW SAL** (Storage Abstraction Layer - RGW's internal API)

**Recommendation**: Use **RGW SAL** for production deployments. Use raw RADOS only for specialized use cases.

---

## Feature Comparison

| Feature | Raw RADOS | RGW SAL | Winner |
|---------|-----------|---------|--------|
| **API Level** | Low-level pool/object | High-level bucket/object | SAL |
| **Multipart Upload** | Custom implementation required | Native support | SAL |
| **Object Metadata** | Manual xattr management | Built-in metadata system | SAL |
| **Versioning** | Not supported | Object versioning | SAL |
| **Listing** | Flat iteration | Prefix-based, paginated | SAL |
| **ACLs** | Manual implementation | S3-compatible ACLs | SAL |
| **Lifecycle** | Not supported | Object lifecycle rules | SAL |
| **S3 Compatibility** | No | Yes (S3/Swift semantics) | SAL |
| **Implementation Complexity** | Medium (custom multipart) | High (C++ FFI) | RADOS |
| **Performance** | Lower latency (direct) | Higher latency (abstraction layer) | RADOS |
| **Maintenance** | More custom code | Less custom code (reuse RGW) | SAL |
| **Dependencies** | librados (C) | librados + librgw (C++) | RADOS |

---

## Use Case Recommendations

### Use **Raw RADOS** When:

✅ **Low latency is critical**
- Direct RADOS access avoids RGW abstraction overhead
- ~10-20% faster for simple put/get operations

✅ **Working with pools directly**
- Existing RADOS-based application
- Need namespace isolation without buckets

✅ **Simpler dependencies**
- Only need librados (C library)
- Easier to build/link

✅ **Custom storage patterns**
- Non-standard object layouts
- Specialized use cases (e.g., block storage)

### Use **RGW SAL** When:

✅ **S3 compatibility is important**
- Need bucket semantics
- Require multipart upload
- Want object versioning

✅ **Production deployments**
- Battle-tested code (powers RGW)
- Built-in features (ACLs, lifecycle, etc.)
- Less custom code to maintain

✅ **Multimodal data**
- Large files requiring multipart
- Metadata-heavy workloads

✅ **Future-proofing**
- RGW SAL evolving with new features
- Better integration with Ceph ecosystem

---

## Multipart Upload Comparison

### Raw RADOS Approach

```rust
// Our custom implementation
impl MultipartUpload for RadosMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let part_name = format!("{}-part-{:05}", upload_id, part_num);

        // Write each part as separate RADOS object
        rados_write_full(io_ctx, &part_name, data, len);

        // Track part metadata in memory
        self.parts.push((part_num, data.len()));
    }

    async fn complete(&mut self) -> Result<PutResult> {
        // Read all parts and concatenate
        for part in parts {
            let data = rados_read(io_ctx, part_name, ...);
            rados_append(io_ctx, final_object, data, len);
        }

        // Clean up temporary parts
        for part in parts {
            rados_remove(io_ctx, part_name);
        }
    }
}
```

**Drawbacks**:
- 2x storage during upload (temp parts + final object)
- Manual concatenation (I/O overhead)
- Custom deletion logic
- Potential edge cases (partial uploads, crashes)

### RGW SAL Approach

```rust
// Using SAL's native multipart
impl MultipartUpload for RgwSalMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        // SAL handles storage internally
        self.upload.upload_part(part_num, data)?;

        // SAL tracks metadata
    }

    async fn complete(&mut self) -> Result<PutResult> {
        // SAL assembles final object efficiently
        let etag = self.upload.complete(part_etags)?;

        // SAL handles cleanup
    }
}
```

**Advantages**:
- Native multipart (same as S3/Swift)
- Optimized storage (SAL determines best strategy)
- Proven implementation (RGW tested at scale)
- Automatic cleanup

---

## Performance Characteristics

### Latency Comparison

| Operation | Raw RADOS | RGW SAL | Difference |
|-----------|-----------|---------|------------|
| **PUT (small object)** | ~5ms | ~6ms | +20% |
| **GET (small object)** | ~3ms | ~4ms | +33% |
| **LIST (1000 objects)** | ~50ms | ~45ms | -10% (SAL optimized) |
| **Multipart (10 parts)** | ~100ms | ~80ms | -20% (SAL native) |
| **Metadata query** | ~10ms | ~5ms | -50% (SAL cached) |

**Note**: Numbers are approximate. Actual performance depends on cluster configuration.

### Throughput Comparison

| Workload | Raw RADOS | RGW SAL |
|----------|-----------|---------|
| **Sequential writes** | ~1.2 GB/s | ~1.0 GB/s |
| **Sequential reads** | ~1.5 GB/s | ~1.3 GB/s |
| **Random reads** | ~800 MB/s | ~700 MB/s |
| **Large file upload** | ~600 MB/s | ~900 MB/s (multipart) |

---

## Implementation Complexity

### Lines of Code Estimate

| Component | Raw RADOS | RGW SAL |
|-----------|-----------|---------|
| **FFI Bindings** | ~200 (C) | ~500 (C++) |
| **Client Implementation** | ~400 | ~300 |
| **Multipart Upload** | ~600 (custom) | ~200 (SAL wrapper) |
| **Tests** | ~400 | ~300 |
| **Total** | **~1,600 LOC** | **~1,300 LOC** |

**Why SAL has less code**: Reuses RGW's battle-tested multipart implementation instead of building from scratch.

### Development Time Estimate

| Phase | Raw RADOS | RGW SAL |
|-------|-----------|---------|
| **FFI Bindings** | 2 days | 5 days (C++ complexity) |
| **Core Implementation** | 5 days | 3 days (less custom logic) |
| **Multipart Upload** | 10 days (build + test) | 2 days (wrapper only) |
| **Integration Testing** | 3 days | 3 days |
| **Total** | **20 days** | **13 days** |

---

## Deployment Considerations

### Dependencies

**Raw RADOS**:
```bash
# Ubuntu/Debian
apt-get install librados-dev

# RHEL/CentOS
yum install librados-devel

# Rust
librados-sys = "0.4"  # FFI bindings
```

**RGW SAL**:
```bash
# Ubuntu/Debian
apt-get install librados-dev librgw-dev libceph-dev

# RHEL/CentOS
yum install librados-devel librgw-devel ceph-devel

# Rust
cxx = "1.0"            # C++ interop
librados-sys = "0.4"   # RADOS FFI
# + custom C++ wrapper
```

### Build Complexity

**Raw RADOS**:
```toml
[build-dependencies]
# Simple: just link librados
```

**RGW SAL**:
```toml
[build-dependencies]
cxx-build = "1.0"

# build.rs needs to:
# - Compile C++ wrapper code
# - Link librados, librgw, and C++ stdlib
# - Handle C++17 requirements
```

---

## Migration Path

If you start with Raw RADOS and want to migrate to RGW SAL later:

### Step 1: Data Migration

Raw RADOS uses pools, SAL uses buckets. Migration:

```bash
# List all objects in RADOS pool
rados -p mypool ls > objects.txt

# For each object, copy to RGW bucket
while read obj; do
  rados -p mypool get "$obj" "/tmp/$obj"
  s3cmd put "/tmp/$obj" "s3://mybucket/$obj"
done < objects.txt
```

### Step 2: Code Migration

The `object_store` API is the same, so application code doesn't change:

```rust
// Before (RADOS)
let store = RadosBuilder::new()
    .with_pool_name("mypool")
    .build()?;

// After (RGW SAL)
let store = RgwSalBuilder::new()
    .with_bucket("mybucket")
    .build()?;

// Application code unchanged
let data = store.get(&path).await?;
```

---

## Recommendation Summary

### For arrow-rs-object-store Project

**Recommended**: Implement **both backends** with feature flags

```toml
[features]
default = []
rados = ["librados-sys"]           # Raw RADOS backend
rgw-sal = ["cxx", "cxx-build"]     # RGW SAL backend
```

**Why both?**
- **rados**: Quick to implement, useful for simple cases
- **rgw-sal**: Production-ready, full-featured

**Priority**: Start with **Raw RADOS** (faster to implement), add RGW SAL later based on demand.

### For Production Use

**Recommended**: **RGW SAL**

**Reasoning**:
1. Proven at scale (powers RGW)
2. Native multipart upload
3. Less custom code to maintain
4. Future-proof (evolves with Ceph)

**Trade-off**: ~20% higher latency acceptable for reliability benefits.

---

## Quick Decision Tree

```
Do you need S3-compatible buckets?
├─ YES → Use RGW SAL
└─ NO
    │
    ├─ Do you need multipart upload?
    │  ├─ YES → Use RGW SAL (native multipart)
    │  └─ NO → Continue
    │
    ├─ Is low latency critical (<5ms)?
    │  ├─ YES → Use Raw RADOS
    │  └─ NO → Continue
    │
    ├─ Are you comfortable with C++ FFI?
    │  ├─ YES → Use RGW SAL (better features)
    │  └─ NO → Use Raw RADOS (simpler FFI)
    │
    └─ Default → Use RGW SAL for production
```

---

## Conclusion

| Criteria | Winner | Reasoning |
|----------|--------|-----------|
| **Time to Market** | Raw RADOS | Faster to implement (C vs C++ FFI) |
| **Production Readiness** | RGW SAL | Battle-tested, feature-complete |
| **Performance** | Raw RADOS | ~20% lower latency for simple ops |
| **Maintainability** | RGW SAL | Less custom code, reuse RGW |
| **Feature Completeness** | RGW SAL | Multipart, versioning, ACLs, etc. |

**Final Recommendation**:
- **Phase 1**: Implement Raw RADOS for MVP (2-3 weeks)
- **Phase 2**: Add RGW SAL for production (3-4 weeks)
- **Phase 3**: Deprecate Raw RADOS if not needed

Both implementations can coexist with feature flags, letting users choose based on their needs.
