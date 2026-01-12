# RADOS Multipart Upload Implementation

This document explains how multipart uploads are handled in the Ceph RADOS backend for arrow-rs-object-store.

## Problem Statement

**Ceph RADOS does not have native multipart upload support** like AWS S3. S3's multipart upload API provides:

- Ability to upload large objects in parts
- Resumable uploads
- Parallel part uploads
- Atomic completion/abort operations

We need to implement equivalent functionality using RADOS primitives.

## Implementation Design

### Architecture

Since RADOS lacks native multipart upload, we implement it using:

1. **Temporary Part Objects**: Each uploaded part is stored as a separate RADOS object
2. **Upload Session ID**: A UUID identifies each multipart upload session
3. **Part Naming Convention**: `<upload_id>-part-<part_number>`
4. **Metadata Object**: Optional tracking object for upload state
5. **Final Assembly**: On complete, concatenate parts into the final object
6. **Cleanup**: On abort, delete all temporary parts

### Object Naming

```
Original object: "data/myfile.bin"
Upload ID: "550e8400-e29b-41d4-a716-446655440000"

Part objects created:
  - 550e8400-e29b-41d4-a716-446655440000-part-00000
  - 550e8400-e29b-41d4-a716-446655440000-part-00001
  - 550e8400-e29b-41d4-a716-446655440000-part-00002
  ...

Metadata object (optional):
  - 550e8400-e29b-41d4-a716-446655440000-metadata
```

### Upload Workflow

```
┌─────────────────────────────────────────────────────────────┐
│                    1. Initialize Upload                     │
│  - Generate unique upload ID (UUID)                        │
│  - Return RadosMultipartUpload instance                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    2. Upload Parts (Parallel)                │
│  For each part:                                             │
│    - Create part object: <upload_id>-part-<N>              │
│    - Write data with rados_write_full()                    │
│    - Store metadata with rados_setxattr()                  │
│    - Track part number and size                            │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────┴─────────┐
                    │                   │
                    ▼                   ▼
        ┌───────────────────┐  ┌────────────────┐
        │   3a. Complete    │  │   3b. Abort    │
        └───────────────────┘  └────────────────┘
                    │                   │
                    ▼                   ▼
    ┌────────────────────────┐  ┌──────────────────┐
    │ - Read each part       │  │ - Delete all     │
    │ - Concatenate into     │  │   part objects   │
    │   final object with    │  │ - Delete         │
    │   rados_append() or    │  │   metadata       │
    │   rados_write()        │  │                  │
    │ - Delete parts         │  └──────────────────┘
    │ - Set final metadata   │
    └────────────────────────┘
```

## RADOS Operations Used

### Part Upload (`put_part`)

```rust
// Write part data as a separate object
rados_write_full(io_ctx, part_object_name, data, data_len);

// Store part metadata using extended attributes
rados_setxattr(io_ctx, part_object_name, "part_number", part_idx);
rados_setxattr(io_ctx, part_object_name, "upload_id", upload_id);
```

### Complete Upload (`complete`)

**Option 1: Using rados_append (Simpler)**
```rust
// Create final object (initially empty or with first part)
rados_write_full(io_ctx, final_object_name, part_0_data, part_0_len);

// Append remaining parts
for part in parts[1..] {
    let part_data = rados_read(io_ctx, part_object_name, ...);
    rados_append(io_ctx, final_object_name, part_data, part_len);
}
```

**Option 2: Using rados_write with offsets (More control)**
```rust
let mut offset = 0;
for part in parts {
    let part_data = rados_read(io_ctx, part_object_name, ...);
    rados_write(io_ctx, final_object_name, part_data, part_len, offset);
    offset += part_len;
}
```

**Option 3: Using RADOS Striper (Best performance)**
```c
// For very large objects, use the RADOS Striper API
rados_striper_create(io_ctx, &striper);
rados_striper_write_full(striper, final_object_name, data, len);
```

### Abort Upload (`abort`)

```rust
// Delete each part object
for part_name in part_objects {
    rados_remove(io_ctx, part_name);
}

// Delete metadata object
rados_remove(io_ctx, metadata_object_name);
```

## Implementation Details

### RadosMultipartUpload Struct

```rust
pub struct RadosMultipartUpload {
    /// Unique identifier for this upload session
    upload_id: String,
    /// Final destination path for the object
    location: Path,
    /// Pool name
    pool_name: String,
    /// Optional namespace
    namespace: Option<String>,
    /// Connection parameters
    cluster_name: String,
    user_name: String,
    conf_file: Option<String>,
    keyring: Option<String>,
    key: Option<String>,
    /// Uploaded parts tracking (part_number -> size)
    parts: Arc<Mutex<Vec<(usize, usize)>>>,
}
```

### Part Tracking

Parts are tracked in memory during the upload session:

```rust
parts: Arc<Mutex<Vec<(usize, usize)>>>
//                    ├────┬────┘
//                    │    └─ Size in bytes
//                    └─ Part number (sequential)
```

This allows us to:
- Know which parts have been uploaded
- Maintain part order for concatenation
- Calculate total size
- Validate completeness

### Metadata Storage (Optional)

For resumable uploads or crash recovery, we can store upload state in RADOS:

```rust
// Metadata object structure (JSON)
{
  "upload_id": "550e8400-e29b-41d4-a716-446655440000",
  "location": "data/myfile.bin",
  "pool": "my-pool",
  "namespace": "app",
  "created_at": "2026-01-07T12:00:00Z",
  "parts": [
    {"number": 0, "size": 5242880, "etag": "..."},
    {"number": 1, "size": 5242880, "etag": "..."}
  ]
}
```

## Usage Example

```rust
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use object_store::rados::RadosBuilder;
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create RADOS store
    let store = RadosBuilder::new()
        .with_pool_name("my-pool")
        .with_user_name("client.myapp")
        .build()?;

    let location = object_store::path::Path::from("large-file.bin");

    // Initiate multipart upload
    let mut upload = store.put_multipart(&location).await?;

    // Upload parts (can be done in parallel)
    let part1 = Bytes::from(vec![0u8; 5 * 1024 * 1024]); // 5 MB
    let part2 = Bytes::from(vec![1u8; 5 * 1024 * 1024]);
    let part3 = Bytes::from(vec![2u8; 3 * 1024 * 1024]); // 3 MB

    upload.put_part(part1.into()).await?;
    upload.put_part(part2.into()).await?;
    upload.put_part(part3.into()).await?;

    // Complete the upload
    upload.complete().await?;

    println!("Multipart upload completed!");
    Ok(())
}
```

## Advantages of This Approach

### ✅ Pros

1. **No External Dependencies**: Uses only standard RADOS operations
2. **Atomic Operations**: RADOS write operations are atomic
3. **Parallel Uploads**: Parts can be uploaded concurrently
4. **Namespace Support**: Works with RADOS namespaces
5. **Cleanup on Abort**: Proper resource cleanup
6. **Standard API**: Implements object_store::MultipartUpload trait

### ⚠️ Cons

1. **No Native Support**: Requires concatenation step (overhead)
2. **Temporary Objects**: Creates additional objects (storage overhead)
3. **No Resumability**: Upload state not persisted (can be added)
4. **Sequential Assembly**: Parts must be concatenated in order
5. **Memory Overhead**: Parts tracked in memory during upload

## Alternative Approaches Considered

### Approach 1: RADOS Striper

Use the RADOS Striper API for automatic striping:

```c
rados_striper_t striper;
rados_striper_create(io_ctx, &striper);
rados_striper_set_object_layout_stripe_unit(striper, 4194304); // 4MB
rados_striper_write(striper, oid, data, len, offset);
```

**Pros**: Better performance, automatic striping
**Cons**: More complex API, less control over part boundaries

### Approach 2: Direct Offset Writes

Write parts directly to final object at calculated offsets:

```rust
// No temporary objects, write directly
rados_write(io_ctx, final_object, part_data, len, offset);
```

**Pros**: No concatenation step, no temporary objects
**Cons**: Requires knowing all part offsets upfront, less flexible

### Approach 3: Use RBD (RADOS Block Device)

If storing very large files, consider using RBD instead:

**Pros**: Better suited for large objects, native block device semantics
**Cons**: Different API, requires RBD setup

## Performance Considerations

### Optimize Concatenation

For large uploads:

```rust
// Use larger buffer sizes
const CONCAT_BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8 MB

// Parallel read of parts (if possible)
let part_futures: Vec<_> = parts.iter()
    .map(|part| read_part_async(part))
    .collect();
let part_data = futures::future::join_all(part_futures).await;

// Sequential write (must maintain order)
for data in part_data {
    rados_append(io_ctx, final_object, data, len);
}
```

### Connection Pooling

Reuse RADOS connections across multipart upload operations:

```rust
pub struct RadosConnectionPool {
    connections: Arc<Mutex<Vec<RadosConnection>>>,
    max_size: usize,
}
```

### Async I/O

Use RADOS AIO for better performance:

```rust
// Instead of blocking writes
rados_write_full(io_ctx, object, data, len);

// Use async I/O
rados_aio_create_completion(..., &completion);
rados_aio_write_full(io_ctx, object, completion, data, len);
rados_aio_wait_for_complete(completion);
```

## Testing

### Unit Tests

Test part naming, metadata creation, and state tracking:

```rust
#[test]
fn test_part_naming() {
    let upload = RadosMultipartUpload::new(...);
    assert_eq!(upload.part_object_name(0), "...-part-00000");
}
```

### Integration Tests

Test against real Ceph cluster:

```rust
#[tokio::test]
async fn test_multipart_upload() {
    let store = RadosBuilder::from_env().build()?;
    let mut upload = store.put_multipart(&path).await?;

    // Upload parts
    for i in 0..5 {
        let data = vec![i as u8; 1024 * 1024]; // 1 MB per part
        upload.put_part(data.into()).await?;
    }

    upload.complete().await?;

    // Verify final object
    let result = store.get(&path).await?;
    assert_eq!(result.meta.size, 5 * 1024 * 1024);
}
```

## Future Enhancements

1. **Resumable Uploads**: Persist upload state to metadata object
2. **Part Validation**: Checksum verification for each part
3. **Parallel Assembly**: Concurrent reads when building final object
4. **Compression**: Optional compression of parts
5. **Encryption**: Support for encrypted objects
6. **Quota Management**: Track storage usage for multipart uploads
7. **Garbage Collection**: Clean up abandoned uploads
8. **Progress Tracking**: Report upload progress

## References

- [RADOS librados API](https://docs.ceph.com/en/latest/rados/api/librados/)
- [S3 Multipart Upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html)
- [RADOS Striper](https://docs.ceph.com/en/latest/man/8/rados/)
- [object_store MultipartUpload trait](https://docs.rs/object_store/latest/object_store/multipart/trait.MultipartUpload.html)
