# ObjectStore API Implementation Matrix

This document provides a detailed analysis of all APIs/operations required by storage providers in the `object_store` crate and how each backend implements them.

## Table of Contents

1. [Required ObjectStore Trait Methods](#required-objectstore-trait-methods)
2. [Core Operations Overview](#core-operations-overview)
3. [Backend Implementation Matrix](#backend-implementation-matrix)
4. [Detailed Implementation Analysis](#detailed-implementation-analysis)
5. [Atomic Operations & Guarantees](#atomic-operations--guarantees)

---

## Required ObjectStore Trait Methods

All backends must implement the `ObjectStore` trait defined in `src/lib.rs`. Here are the **required** methods:

### Core Methods (Required)

| Method | Signature | Description |
|--------|-----------|-------------|
| `put_opts` | `async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult>` | **Atomic** write operation. Must be atomic (all-or-nothing). |
| `put_multipart_opts` | `async fn put_multipart_opts(&self, location: &Path, opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>>` | Initiate multipart upload for large objects. |
| `get_opts` | `async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult>` | Read object with options (range, conditional get, etc.). |
| `get_ranges` | `async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>>` | Vectored read (multiple ranges). |
| `delete_stream` | `fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>>` | Delete multiple objects (may use bulk ops). |
| `list` | `fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>>` | List objects with prefix (recursive). |
| `list_with_delimiter` | `async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult>` | List with delimiter (non-recursive, returns common prefixes). |
| `copy_opts` | `async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()>` | Copy object within same store. |
| `rename_opts` | `async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()>` | Move/rename object (default: copy + delete). |

### Optional Methods (Have Default Implementations)

| Method | Default Implementation | Can Override? |
|--------|----------------------|---------------|
| `get_ranges` | Calls `get_range` for each range with coalescing | ✅ Yes (for optimization) |
| `list_with_offset` | Filters `list()` results | ✅ Yes (for optimization) |
| `rename_opts` | `copy_opts` + `delete` | ✅ Yes (for atomic rename) |

---

## Core Operations Overview

### 1. **Put Operations**

#### `put_opts` - Atomic Write

**PutOptions includes:**
- `mode: PutMode` - Controls overwrite/create/update behavior
- `tags: TagSet` - Object tags (cloud-specific)
- `attributes: Attributes` - Content-Type, Cache-Control, etc.
- `extensions: Extensions` - Implementation-specific context

**PutMode variants:**
```rust
pub enum PutMode {
    Overwrite,           // Overwrite any existing object
    Create,              // Fail if object exists (Error::AlreadyExists)
    Update(UpdateVersion), // Conditional update (check ETag/version)
}
```

**Atomicity guarantee:** The operation must be atomic - either the entire payload is written, or nothing is written. Partial writes must not be observable.

#### `put_multipart_opts` - Streaming Upload

For large objects where buffering entire payload is impractical.

**Returns:** `Box<dyn MultipartUpload>` trait object with:
- `put_part(&mut self, data: PutPayload) -> UploadPart` - Upload a part
- `complete(&mut self) -> Result<PutResult>` - Finalize upload
- `abort(&mut self) -> Result<()>` - Cancel upload

---

### 2. **Get Operations**

#### `get_opts` - Read with Options

**GetOptions includes:**
- `if_match: Option<String>` - ETag must match
- `if_none_match: Option<String>` - ETag must not match
- `if_modified_since: Option<DateTime<Utc>>` - Modified since timestamp
- `if_unmodified_since: Option<DateTime<Utc>>` - Not modified since timestamp
- `range: Option<GetRange>` - Byte range to read
- `version: Option<String>` - Specific version (for versioned stores)
- `head: bool` - Return only metadata (no data)

**GetResult includes:**
- `payload: GetResultPayload` - Stream of bytes
- `meta: ObjectMeta` - Object metadata
- `range: Range<usize>` - Actual range returned
- `attributes: Attributes` - Object attributes

#### `get_ranges` - Vectored Read

Read multiple (potentially non-contiguous) byte ranges in one operation. Default implementation coalesces adjacent ranges.

---

### 3. **Delete Operations**

#### `delete_stream` - Bulk Delete

**Must implement:** Each backend must provide its own implementation (no default since v0.13).

**Bulk delete support:**
- **AWS S3**: Native `DeleteObjects` API (up to 1000 objects/batch)
- **Azure**: Native `Blob Batch` API (up to 256 objects/batch)
- **GCP, HTTP, Local, Memory**: Concurrent individual deletes

---

### 4. **List Operations**

#### `list` - Recursive Listing

List all objects with given prefix. Results are **not guaranteed to be ordered**.

#### `list_with_delimiter` - Hierarchical Listing

Returns:
- `objects: Vec<ObjectMeta>` - Objects directly under prefix
- `common_prefixes: Vec<Path>` - "Subdirectories" under prefix

---

### 5. **Copy Operations**

#### `copy_opts` - Server-Side Copy

**CopyOptions includes:**
- `mode: CopyMode` - Overwrite or Create

**CopyMode variants:**
```rust
pub enum CopyMode {
    Overwrite,  // Overwrite target if exists
    Create,     // Fail if target exists (Error::AlreadyExists)
}
```

**Implementation approaches:**
1. **Native copy** - Server-side copy API (S3, GCS, Azure)
2. **Get + Put** - Download then upload (fallback)

---

### 6. **Rename Operations**

#### `rename_opts` - Move/Rename

**RenameOptions includes:**
- `target_mode: RenameTargetMode` - Overwrite or Create

**Default implementation:** `copy_opts` + `delete`

**Atomicity:** Default is **NOT atomic** - source may still exist if delete fails. Some backends (e.g., local filesystem) can provide atomic rename.

---

## Backend Implementation Matrix

| Backend | put_opts | Multipart | copy_opts | rename_opts | Atomic Put | Atomic Copy | Atomic Rename | Bulk Delete |
|---------|----------|-----------|-----------|-------------|------------|-------------|---------------|-------------|
| **AWS S3** | ✅ Native | ✅ Native | ✅ Native | 🔄 Copy+Del | ✅ Yes | ⚠️ Eventual | ❌ No | ✅ Native (1000/batch) |
| **Azure Blob** | ✅ Native | ✅ Native | ✅ Native | 🔄 Copy+Del | ✅ Yes | ⚠️ Eventual | ❌ No | ✅ Native (256/batch) |
| **GCP GCS** | ✅ Native | ✅ Native | ✅ Native | 🔄 Copy+Del | ✅ Yes | ⚠️ Eventual | ❌ No | 🔄 Concurrent (10) |
| **Local FS** | ✅ Native | ✅ Emulated | ✅ Hard Link | ✅ fs::rename | ✅ Yes | ✅ Yes | ✅ Yes (Overwrite) | 🔄 Concurrent (10) |
| **Memory** | ✅ In-Mem | ✅ In-Mem | ✅ In-Mem | 🔄 Copy+Del | ✅ Yes | ✅ Yes | ❌ No | 🔄 Sequential |
| **HTTP** | ✅ PUT | ✅ PUT | 🔄 Get+Put | 🔄 Copy+Del | ✅ Yes | ❌ No | ❌ No | 🔄 Concurrent (10) |
| **RADOS** | ✅ Native | ✅ Custom | ❌ Get+Put* | 🔄 Copy+Del | ✅ Yes | ❌ No | ❌ No | 🔄 Stream |
| **RGW SAL** | ✅ Native | ✅ Native | ✅ Native* | 🔄 Copy+Del | ✅ Yes | ⚠️ Eventual | ❌ No | 🔄 Stream |

**Legend:**
- ✅ **Native** - Backend provides native API
- 🔄 **Emulated** - Implemented via other operations
- ❌ **No** - Not natively supported
- ⚠️ **Eventual** - Eventually consistent (not immediately atomic)
- *Planned but not yet implemented

---

## Detailed Implementation Analysis

### AWS S3

**Location:** `src/aws/mod.rs`, `src/aws/client.rs`

#### put_opts
```rust
async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult>
```

**Implementation:**
- `PutMode::Overwrite` → Standard `PUT` request
- `PutMode::Create` → `PUT` with `If-None-Match: *` header (requires S3 conditional put support)
- `PutMode::Update(v)` → `PUT` with `If-Match: <etag>` header

**Atomicity:** ✅ S3 guarantees atomic PUT operations

**Conditional Put Support:**
- Requires `S3ConditionalPut::ETagMatch` configuration
- Not all S3-compatible stores support this (e.g., MinIO may not)
- Falls back to `Error::NotImplemented` if disabled

#### copy_opts
```rust
async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()>
```

**Implementation:**
- `CopyMode::Overwrite` → Standard `CopyObject` API
- `CopyMode::Create` → Two strategies:
  1. **Header-based** (if `S3CopyIfNotExists::Header`): Custom header + check `PRECONDITION_FAILED`
  2. **Multipart-based** (if `S3CopyIfNotExists::Multipart`):
     - Initiate multipart upload
     - Upload part via copy (CopyPart API)
     - Complete multipart with create mode
     - Abort on `Error::Precondition`

**Atomicity:** ⚠️ Eventually consistent (S3 guarantees eventual consistency)

**Special cases:**
- R2, MinIO may return different status codes for conditional failures

#### rename_opts
**Default implementation:** `copy_opts` + `delete`

**Atomicity:** ❌ Not atomic - source may remain if delete fails

#### delete_stream
**Implementation:** Native `DeleteObjects` API
- Batches up to 1000 objects per request
- Returns errors for individual failures

---

### Azure Blob Storage

**Location:** `src/azure/mod.rs`, `src/azure/client.rs`

#### put_opts
```rust
async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult>
```

**Implementation:**
- Uses `PUT Blob` API
- Supports conditional operations via `If-None-Match` and `If-Match` headers

**Atomicity:** ✅ Azure guarantees atomic blob creation

#### copy_opts
```rust
async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()>
```

**Implementation:**
- `CopyMode::Overwrite` → `Copy Blob` API with overwrite allowed
- `CopyMode::Create` → `Copy Blob` API with `If-None-Match: *`

**Atomicity:** ⚠️ Eventually consistent for list operations

#### rename_opts
**Default implementation:** `copy_opts` + `delete`

**Atomicity:** ❌ Not atomic

#### delete_stream
**Implementation:** Native `Blob Batch` API
- Batches up to 256 blobs per request
- More limited than S3 but still native

---

### GCP Cloud Storage

**Location:** `src/gcp/mod.rs`, `src/gcp/client.rs`

#### put_opts
```rust
async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult>
```

**Implementation:**
- Uses GCS `multipart/related` upload or simple upload
- Supports conditional operations via `If-Generation-Match: 0` (for create)

**Atomicity:** ✅ GCS guarantees atomic object creation

#### copy_opts
```rust
async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()>
```

**Implementation:**
- `CopyMode::Overwrite` → GCS `rewriteTo` API
- `CopyMode::Create` → GCS `rewriteTo` with `If-Generation-Match: 0`

**Atomicity:** ⚠️ Eventually consistent

#### rename_opts
**Default implementation:** `copy_opts` + `delete`

**Atomicity:** ❌ Not atomic

#### delete_stream
**Implementation:** Concurrent individual deletes (up to 10 concurrent)

---

### Local Filesystem

**Location:** `src/local.rs`

#### put_opts
```rust
async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult>
```

**Implementation:**
1. Write to staging file (`.{filename}.{uuid}`)
2. `PutMode::Overwrite` → `fs::rename(staging, target)` (atomic)
3. `PutMode::Create` → `fs::hard_link(staging, target)` then `fs::remove(staging)`
   - `hard_link` fails with `AlreadyExists` if target exists
4. `PutMode::Update` → ❌ Not implemented

**Atomicity:** ✅ Yes
- `fs::rename` is atomic on POSIX systems (same filesystem)
- `hard_link` + check for `AlreadyExists` ensures create-only semantics

**Limitations:**
- Staging and target must be on same filesystem for atomic rename
- `Update` mode not supported (no etag concept on filesystem)

#### copy_opts
```rust
async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()>
```

**Implementation:**
- `CopyMode::Overwrite`:
  1. `hard_link(from, staging.{id})`
  2. `fs::rename(staging, to)` - atomic
- `CopyMode::Create`:
  1. `hard_link(from, to)` - fails if target exists

**Atomicity:** ✅ Yes (via hard link + atomic rename)

**Why hard link?**
- Efficient (no data copy)
- Works for copy-on-write filesystems
- Atomic for Create mode

#### rename_opts
```rust
async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()>
```

**Implementation:**
- `RenameTargetMode::Overwrite` → `fs::rename(from, to)` - **atomic**!
- `RenameTargetMode::Create` → `copy_opts(Create)` + `delete`

**Atomicity:**
- ✅ **Overwrite mode is atomic** (POSIX guarantee)
- ❌ Create mode is not atomic (copy + delete)

**This is the ONLY backend with atomic rename for Overwrite mode!**

#### delete_stream
**Implementation:** Concurrent individual deletes (up to 10 concurrent)

---

### Memory (In-Memory)

**Location:** `src/memory.rs`

#### put_opts
```rust
async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult>
```

**Implementation:**
- All modes supported via HashMap operations:
  - `Overwrite` → `map.insert()`
  - `Create` → Check existence, fail if present
  - `Update` → Check etag, fail if mismatch

**Atomicity:** ✅ Yes (in-memory operations are atomic within RwLock)

#### copy_opts
```rust
async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()>
```

**Implementation:**
- Simple HashMap lookup + insert
- No actual data copy (Arc cloning)

**Atomicity:** ✅ Yes (protected by RwLock)

#### rename_opts
**Default implementation:** `copy_opts` + `delete`

**Atomicity:** ❌ Not atomic (two separate lock acquisitions)

#### delete_stream
**Implementation:** Sequential deletes (one at a time)

---

### HTTP / WebDAV

**Location:** `src/http/mod.rs`, `src/http/client.rs`

#### put_opts
```rust
async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult>
```

**Implementation:**
- Standard HTTP `PUT` request
- May support `If-None-Match: *` for Create mode (server-dependent)
- `Update` mode generally not supported (no standard HTTP etag-based update)

**Atomicity:** ✅ Server-dependent, but HTTP PUT is typically atomic

**Limitations:**
- Conditional operations depend on server support
- Not all HTTP servers support WebDAV extensions

#### copy_opts
```rust
async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()>
```

**Implementation:**
- WebDAV `COPY` method if supported
- Fallback to `GET` + `PUT` if COPY not available

**Atomicity:** ❌ No (even with COPY, typically not atomic)

#### rename_opts
**Default implementation:** `copy_opts` + `delete`

**Atomicity:** ❌ No

#### delete_stream
**Implementation:** Concurrent individual `DELETE` requests (up to 10 concurrent)

---

### RADOS (Raw RADOS)

**Location:** `src/rados/mod.rs`, `src/rados/client.rs`

#### put_opts
```rust
async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult>
```

**Implementation:**
- Uses `rados_write_full()` - atomic write operation
- `PutMode::Create` → Check existence with `rados_stat()` first
- Metadata stored as xattrs (`rados_setxattr`)

**Atomicity:** ✅ Yes (RADOS guarantees atomic object writes)

**Limitations:**
- `Update` mode not implemented (would need version tracking)
- Working with pools (not buckets)

#### copy_opts
**Not yet implemented in current codebase**

**Would require:** `rados_read()` + `rados_write_full()` (no native copy)

**Atomicity:** ❌ Not atomic (would be get + put)

#### rename_opts
**Default implementation:** Would use `copy_opts` + `delete` when copy is implemented

**Atomicity:** ❌ No

**Note:** RADOS has no native rename operation

#### delete_stream
**Implementation:** Stream-based individual deletes via `rados_remove()`

---

### RGW SAL (Storage Abstraction Layer)

**Location:** `src/rgw_sal/mod.rs`, `src/rgw_sal/client.rs`

#### put_opts
```rust
async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult>
```

**Implementation:**
- Uses SAL Writer API:
  1. `sal_create_writer()` - Get writer for object
  2. `sal_writer_prepare()` - Prepare write
  3. `sal_writer_write()` - Write data
  4. `sal_writer_complete()` - Finalize (returns ETag)
- `PutMode::Create` → Check with `sal_get_object()` first

**Atomicity:** ✅ Yes (SAL provides atomic writes via Writer interface)

**Advantages over RADOS:**
- Bucket-level abstraction (not just pools)
- Built-in metadata support
- Native multipart upload support

#### copy_opts
**Planned but not yet fully implemented**

**Would use:** SAL's native copy support (when multipart is complete)

**Atomicity:** ⚠️ Eventually consistent (like S3)

#### rename_opts
**Default implementation:** `copy_opts` + `delete` when copy is implemented

**Atomicity:** ❌ No

#### delete_stream
**Implementation:** Stream-based individual deletes via `sal_delete_object()`

---

## Atomic Operations & Guarantees

### Put Operations (Atomicity)

| Backend | Atomicity | Mechanism | Notes |
|---------|-----------|-----------|-------|
| **AWS S3** | ✅ Atomic | S3 PUT API | All-or-nothing write |
| **Azure** | ✅ Atomic | Blob PUT API | All-or-nothing write |
| **GCP** | ✅ Atomic | GCS Upload API | All-or-nothing write |
| **Local FS** | ✅ Atomic | Write to staging, then `fs::rename` | POSIX rename is atomic |
| **Memory** | ✅ Atomic | RwLock-protected HashMap | Single lock acquisition |
| **HTTP** | ⚠️ Server-dependent | HTTP PUT | Typically atomic |
| **RADOS** | ✅ Atomic | `rados_write_full` | RADOS object write is atomic |
| **RGW SAL** | ✅ Atomic | SAL Writer API | Atomic via RGW SAL |

### Copy Operations (Atomicity)

| Backend | Atomicity | Mechanism | Notes |
|---------|-----------|-----------|-------|
| **AWS S3** | ⚠️ Eventually consistent | Server-side `CopyObject` | Copy is atomic, but list may lag |
| **Azure** | ⚠️ Eventually consistent | Server-side `Copy Blob` | Copy is atomic, but list may lag |
| **GCP** | ⚠️ Eventually consistent | Server-side `rewriteTo` | Copy is atomic, but list may lag |
| **Local FS** | ✅ Atomic | `hard_link` + `rename` | Atomic on POSIX |
| **Memory** | ✅ Atomic | HashMap insert | Single lock acquisition |
| **HTTP** | ❌ Not atomic | GET + PUT fallback | Two separate operations |
| **RADOS** | ❌ Not atomic | Would be GET + PUT | No native copy |
| **RGW SAL** | ⚠️ Eventually consistent | Native SAL copy (planned) | Like S3 behavior |

### Rename Operations (Atomicity)

| Backend | Atomicity | Mechanism | Notes |
|---------|-----------|-----------|-------|
| **AWS S3** | ❌ Not atomic | Copy + Delete | Two separate operations |
| **Azure** | ❌ Not atomic | Copy + Delete | Two separate operations |
| **GCP** | ❌ Not atomic | Copy + Delete | Two separate operations |
| **Local FS** | ✅ **Atomic (Overwrite mode)** | `fs::rename` | **ONLY backend with atomic rename!** |
| **Local FS** | ❌ Not atomic (Create mode) | Copy + Delete | Falls back to non-atomic |
| **Memory** | ❌ Not atomic | Copy + Delete | Two lock acquisitions |
| **HTTP** | ❌ Not atomic | Copy + Delete | Two separate operations |
| **RADOS** | ❌ Not atomic | Copy + Delete | Two separate operations |
| **RGW SAL** | ❌ Not atomic | Copy + Delete | Two separate operations |

### Conditional Operations Support

| Backend | PutMode::Create | PutMode::Update | CopyMode::Create | If-Match/If-None-Match |
|---------|-----------------|-----------------|------------------|------------------------|
| **AWS S3** | ✅ If-None-Match: * | ✅ If-Match: etag | ✅ Custom headers | ✅ Full support |
| **Azure** | ✅ If-None-Match: * | ✅ If-Match: etag | ✅ If-None-Match: * | ✅ Full support |
| **GCP** | ✅ If-Generation-Match: 0 | ✅ If-Generation-Match | ✅ If-Generation-Match: 0 | ✅ Generation-based |
| **Local FS** | ✅ hard_link check | ❌ Not supported | ✅ hard_link check | ❌ No etag concept |
| **Memory** | ✅ Existence check | ✅ Etag check | ✅ Existence check | ✅ In-memory checks |
| **HTTP** | ⚠️ Server-dependent | ⚠️ Server-dependent | ⚠️ Server-dependent | ⚠️ Server-dependent |
| **RADOS** | ✅ rados_stat check | ❌ Not implemented | ❌ Not implemented | ⚠️ Partial (via xattrs) |
| **RGW SAL** | ✅ SAL object check | ⚠️ Planned | ⚠️ Planned | ✅ Via SAL metadata |

---

## Summary: Operation Support by Backend

### ✅ Fully Implemented Operations

| Operation | AWS | Azure | GCP | Local | Memory | HTTP | RADOS | SAL |
|-----------|-----|-------|-----|-------|--------|------|-------|-----|
| **put_opts** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **put_multipart_opts** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **get_opts** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **delete_stream** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **list** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **list_with_delimiter** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **copy_opts** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ❌* | ⚠️† |

**Legend:**
- ✅ = Fully implemented
- ⚠️ = Partially implemented or planned
- ❌ = Not implemented
- * = Would require GET+PUT implementation
- † = Planned with SAL native support

### 🔒 Atomic Operation Support

| Operation | AWS | Azure | GCP | Local | Memory | HTTP | RADOS | SAL |
|-----------|-----|-------|-----|-------|--------|------|-------|-----|
| **Atomic Put** | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ | ✅ | ✅ |
| **Atomic Copy** | ⚠️ | ⚠️ | ⚠️ | ✅ | ✅ | ❌ | ❌ | ⚠️ |
| **Atomic Rename** | ❌ | ❌ | ❌ | ✅* | ❌ | ❌ | ❌ | ❌ |
| **Bulk Delete** | ✅ | ✅ | 🔄 | 🔄 | 🔄 | 🔄 | 🔄 | 🔄 |

**Legend:**
- ✅ = Fully atomic
- ⚠️ = Eventually consistent
- ❌ = Not atomic
- 🔄 = Concurrent individual ops
- * = Only for Overwrite mode

---

## Key Takeaways

1. **Local Filesystem is the ONLY backend with atomic rename** (for Overwrite mode)
2. **All backends provide atomic PUT operations**
3. **Cloud providers (S3, Azure, GCP) are eventually consistent** for copy operations
4. **RADOS lacks native copy** - would need to implement via GET+PUT
5. **SAL provides better abstraction** than raw RADOS (buckets vs pools, native multipart)
6. **Conditional operations require special configuration** on S3 (S3ConditionalPut)
7. **Bulk delete is native only on AWS (1000/batch) and Azure (256/batch)**

## Recommendations for New Backend Implementations

When implementing a new storage backend:

1. **Must implement:**
   - `put_opts` with at least Overwrite and Create modes
   - `get_opts` with basic range support
   - `delete_stream` (can be concurrent individual deletes)
   - `list` and `list_with_delimiter`
   - `copy_opts` (can fallback to GET+PUT)

2. **Should implement if backend supports:**
   - Conditional PUT (PutMode::Update)
   - Native server-side copy
   - Bulk delete operations
   - Atomic rename (rare!)

3. **Atomicity guarantees to consider:**
   - PUT must be atomic (all-or-nothing)
   - Copy should be atomic if backend supports it
   - Rename atomicity is backend-dependent (document clearly)

4. **Testing considerations:**
   - Test all PutMode variants
   - Test conditional operations
   - Test copy with existing/non-existing targets
   - Test multipart upload (especially complete/abort)
   - Test concurrent operations
