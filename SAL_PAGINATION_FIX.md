# SAL Backend Pagination Fix

## Problem

The original SAL backend implementation had a critical pagination bug:

- **Single call only**: `list_with_delimiter()` made only ONE call to `sal_list_objects()`
- **Hardcoded limit**: Always requested max 1000 objects
- **Ignored next_marker**: The C API returned `next_marker` for pagination but it was never used
- **Silent data loss**: If a bucket had >1000 objects, only the first 1000 were returned

## Solution

### 1. Fixed `list_with_delimiter()` to Loop Through All Pages

**Location**: `src/rgw_sal/client_v2.rs:307-395`

The method now:
- Loops through all pages until `next_marker` is NULL
- Accumulates all objects from all pages
- Returns complete results regardless of bucket size

**Before**:
```rust
// Made ONE call, returned max 1000 objects
let ret = ffi::sal_list_objects(
    driver,
    bucket_name.as_ptr(),
    prefix_cstr.as_ref().map_or(ptr::null(), |s| s.as_ptr()),
    ptr::null(),  // ← Always NULL (no pagination)
    1000,
    &mut result,
);
```

**After**:
```rust
let mut all_objects = Vec::new();
let mut marker: Option<CString> = None;

// Loop through all pages
loop {
    let ret = ffi::sal_list_objects(
        driver,
        bucket_name.as_ptr(),
        prefix_cstr.as_ref().map_or(ptr::null(), |s| s.as_ptr()),
        marker.as_ref().map_or(ptr::null(), |m| m.as_ptr()),  // ← Use marker
        1000,
        &mut result,
    );

    // ... process results ...

    // Check if there are more pages
    if !result.next_marker.is_null() {
        marker = Some(CString::new(/* next_marker */).unwrap());
        // Continue loop
    } else {
        break;  // No more pages
    }
}
```

### 2. Added `PaginatedListStore` Implementation

**Location**: `src/rgw_sal/client_v2.rs:567-659`

For users who need explicit pagination control:

```rust
#[async_trait]
impl PaginatedListStore for SalClient {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        opts: PaginatedListOptions,
    ) -> Result<PaginatedListResult> {
        // ... implementation ...
    }
}
```

## Usage

### Option 1: Automatic Pagination (Recommended)

Use `list()` - it handles pagination automatically:

```rust
use futures::stream::StreamExt;

let store = SalClient::new(/* ... */)?;

// Automatically fetches ALL objects, regardless of count
let mut stream = store.list(Some(&Path::from("data")));
while let Some(meta) = stream.next().await.transpose()? {
    println!("File: {}", meta.location);
}
```

**Behavior**:
- Fetches ALL objects from bucket
- Loops internally through all pages (1000 objects per page)
- No manual pagination needed

### Option 2: Manual Pagination Control

Use `list_paginated()` for explicit control:

```rust
use object_store::list::{PaginatedListStore, PaginatedListOptions};

let store = SalClient::new(/* ... */)?;
let mut page_token = None;
let mut total = 0;

loop {
    let opts = PaginatedListOptions {
        max_keys: Some(100),  // Fetch 100 at a time
        page_token: page_token.clone(),
        ..Default::default()
    };

    let result = store.list_paginated(Some("data"), opts).await?;
    total += result.result.objects.len();

    println!("Fetched {} objects (total: {})", result.result.objects.len(), total);

    // Process objects...
    for obj in result.result.objects {
        println!("  - {}", obj.location);
    }

    // Check for more pages
    match result.page_token {
        Some(token) => {
            page_token = Some(token);
            println!("Fetching next page...");
        }
        None => {
            println!("All pages fetched!");
            break;
        }
    }
}
```

**When to use**:
- Need to control page size
- Want to show progress between pages
- Building web API with pagination endpoints
- Need stateless pagination (can save/restore token)

## Testing

### Test with Large Bucket

```rust
#[tokio::test]
async fn test_pagination_large_bucket() {
    let store = SalClient::new(/* ... */).unwrap();

    // Create 2500 objects (more than 2 pages)
    for i in 0..2500 {
        store.put(&Path::from(format!("test/obj{:04}", i)), "data".into())
            .await
            .unwrap();
    }

    // List should return ALL objects
    let objects: Vec<_> = store.list(Some(&Path::from("test")))
        .try_collect()
        .await
        .unwrap();

    assert_eq!(objects.len(), 2500, "Should fetch all objects across multiple pages");
}
```

### Test Manual Pagination

```rust
#[tokio::test]
async fn test_manual_pagination() {
    use object_store::list::{PaginatedListStore, PaginatedListOptions};

    let store = SalClient::new(/* ... */).unwrap();

    let opts = PaginatedListOptions {
        max_keys: Some(10),
        ..Default::default()
    };

    let page1 = store.list_paginated(Some("data"), opts.clone()).await.unwrap();
    assert_eq!(page1.result.objects.len(), 10);
    assert!(page1.page_token.is_some(), "Should have next page");

    let opts2 = PaginatedListOptions {
        max_keys: Some(10),
        page_token: page1.page_token,
        ..Default::default()
    };

    let page2 = store.list_paginated(Some("data"), opts2).await.unwrap();
    assert!(page2.result.objects.len() > 0);
}
```

## Performance Considerations

### Memory Usage

**`list_with_delimiter()` (automatic)**:
- Loads ALL objects into memory before returning
- For 10,000 objects: ~1-2 MB RAM (100-200 bytes per ObjectMeta)
- For 100,000 objects: ~10-20 MB RAM
- For 1,000,000 objects: ~100-200 MB RAM

**Recommendation**:
- Use `list()` for buckets with <100k objects
- Use `list_paginated()` for very large buckets

### Network Efficiency

- Each page fetches 1000 objects (hardcoded in SAL C API)
- 10,000 objects = 10 network requests
- Requests are made sequentially (not parallel)
- Each request blocks in `spawn_blocking` thread

### Optimization Ideas (Future Work)

1. **Streaming instead of buffering**: Return stream that fetches pages lazily
2. **Configurable page size**: Make max_keys configurable (currently hardcoded to 1000)
3. **Parallel page fetching**: Fetch multiple pages concurrently
4. **Caching**: Cache list results with TTL

## Files Changed

1. **src/rgw_sal/client_v2.rs**
   - Lines 9-12: Added `list` module imports
   - Lines 307-395: Fixed `list_with_delimiter()` to loop through all pages
   - Lines 567-659: Added `PaginatedListStore` implementation

## Backward Compatibility

✅ **Fully backward compatible**
- All existing code continues to work
- `list()` returns same type (`BoxStream<'static, Result<ObjectMeta>>`)
- Just fetches ALL objects instead of first 1000
- Added new optional `PaginatedListStore` trait

## Summary

| Method | Pagination | Memory | Use Case |
|--------|-----------|---------|----------|
| `list()` | Automatic (loops internally) | Loads all objects | Most common case, <100k objects |
| `list_paginated()` | Manual (with tokens) | One page at a time | Large buckets, progress tracking, web APIs |

Both methods now correctly handle buckets with any number of objects!
