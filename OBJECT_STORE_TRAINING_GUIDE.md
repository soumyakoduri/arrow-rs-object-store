# Object Store Training Guide

**A Comprehensive Guide for New Developers**

---

## Table of Contents

1. [What is object_store?](#what-is-object_store)
2. [Why Does object_store Exist?](#why-does-object_store-exist)
3. [Core Architecture](#core-architecture)
4. [How It Works: Code Flow](#how-it-works-code-flow)
5. [Available Backends](#available-backends)
6. [Getting Started: Using object_store](#getting-started-using-object_store)
7. [Advanced Features](#advanced-features)
8. [How to Add a New Backend](#how-to-add-a-new-backend)
9. [Multipart Upload Deep Dive](#multipart-upload-deep-dive)
10. [Best Practices and Patterns](#best-practices-and-patterns)
11. [Testing Strategy](#testing-strategy)
12. [Common Pitfalls](#common-pitfalls)

---

## What is object_store?

**object_store** is a Rust crate that provides a **unified, high-performance, async API** for interacting with different object storage systems.

### Key Characteristics

- **Trait-based abstraction**: Single `ObjectStore` trait works across all backends
- **Async-first design**: Built on Tokio for non-blocking I/O
- **Production-ready**: Used by [crates.io](https://github.com/rust-lang/crates.io) and [InfluxDB IOx](https://github.com/influxdata/influxdb_iox)
- **Apache Arrow project**: Donated by InfluxData, maintained by Apache Software Foundation
- **Zero-cost abstraction**: No runtime overhead for the abstraction layer

### What It's NOT

- ❌ **Not a filesystem**: No cursor-based APIs like `Read`/`Seek`/`Write`
- ❌ **Not POSIX-compliant**: Designed around object store semantics, not POSIX
- ❌ **Not a database**: No indexing, querying, or transactions
- ❌ **Not a CDN**: No caching layer (though you can add one)

---

## Why Does object_store Exist?

### The Problem

Imagine you're building a data analytics application that needs to:
- Store large files (Parquet, CSV, JSON)
- Run in AWS (S3), Azure (Blob), GCP (GCS), or on-premises
- Support local development without cloud credentials
- Handle multi-terabyte datasets efficiently

**Without object_store**, you would need to:
```rust
// Different code for each backend
#[cfg(feature = "aws")]
use aws_sdk_s3::Client as S3Client;

#[cfg(feature = "azure")]
use azure_storage_blobs::BlobClient;

#[cfg(feature = "gcp")]
use google_cloud_storage::Client as GcsClient;

// Completely different APIs for each!
s3_client.get_object().bucket("my-bucket").key("file.txt").send().await?;
blob_client.get_blob("container", "file.txt").await?;
gcs_client.download_object("bucket", "file.txt").await?;
```

### The Solution

With **object_store**, you write code **once** and it works everywhere:

```rust
use object_store::{ObjectStore, path::Path};

// Works with S3, Azure, GCS, local files, or memory
async fn read_file(store: &dyn ObjectStore, path: &str) -> Result<Bytes> {
    let location = Path::from(path);
    let result = store.get(&location).await?;
    result.bytes().await
}
```

### Benefits

1. **Portability**: Same code runs on AWS, Azure, GCP, local disk, or in-memory
2. **Testability**: Use `InMemory` backend for fast unit tests
3. **Simplified Development**: Local filesystem for dev, cloud for production
4. **Performance**: Each backend optimized for its underlying storage
5. **Safety**: Compile-time guarantees, no runtime reflection

---

## Core Architecture

### The Trait Hierarchy

```
┌─────────────────────────────────────────────────────────────┐
│                      ObjectStore Trait                       │
│  (Core trait - all backends must implement this)            │
│                                                              │
│  Methods:                                                    │
│  • put_opts() - Write object with options                   │
│  • get_opts() - Read object with options                    │
│  • delete_stream() - Delete multiple objects                │
│  • list() - List objects with prefix                        │
│  • put_multipart_opts() - Initiate multipart upload         │
│  • copy_opts() - Copy object                                │
│  • rename_if_not_exists() - Atomic rename                   │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │
                              │ implements
                              │
        ┌─────────────────────┴─────────────────────┐
        │                                           │
        │                                           │
┌───────▼────────┐                         ┌───────▼────────┐
│ ObjectStoreExt │                         │ MultipartUpload│
│  (Extension)   │                         │     (Trait)    │
│                │                         │                │
│  Convenience   │                         │  • put_part()  │
│  methods:      │                         │  • complete()  │
│  • put()       │                         │  • abort()     │
│  • get()       │                         │                │
│  • head()      │                         └────────────────┘
└────────────────┘
```

### Key Traits

#### 1. **ObjectStore** (src/lib.rs:745)

The core trait that all backends implement.

```rust
pub trait ObjectStore: std::fmt::Display + Send + Sync + Debug + 'static {
    /// Save the provided payload to location with options
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult>;

    /// Perform a multipart upload with options
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>>;

    /// Get an object with options (supports ranges, conditional gets, etc.)
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult>;

    /// List objects with optional prefix (returns a stream)
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>>;

    /// Delete multiple objects (takes a stream of paths)
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>>;

    // ... more methods
}
```

**Why this design?**
- **Stateless**: No file handles, cursors, or position tracking
- **Async**: All operations return `Future`s for non-blocking I/O
- **Atomic**: Operations either fully succeed or fully fail
- **Options pattern**: Extensible via `PutOptions`, `GetOptions`, etc.

#### 2. **ObjectStoreExt** (src/lib.rs:1217)

Extension trait providing convenience methods with default options.

```rust
pub trait ObjectStoreExt: ObjectStore {
    // Simpler versions without explicit options
    fn put(&self, location: &Path, payload: PutPayload) -> impl Future<Output = Result<PutResult>>;
    fn get(&self, location: &Path) -> impl Future<Output = Result<GetResult>>;
    fn head(&self, location: &Path) -> impl Future<Output = Result<ObjectMeta>>;
    // ... more
}
```

**Automatically implemented for all `ObjectStore` types.**

#### 3. **MultipartUpload** (src/upload.rs:41)

Interface for uploading large objects in parts.

```rust
#[async_trait]
pub trait MultipartUpload: Send + std::fmt::Debug {
    /// Upload the next part (returns a future)
    fn put_part(&mut self, data: PutPayload) -> UploadPart;

    /// Complete the upload (make all parts visible atomically)
    async fn complete(&mut self) -> Result<PutResult>;

    /// Abort the upload (cleanup partial data)
    async fn abort(&mut self) -> Result<()>;
}
```

### Key Data Types

#### **Path** (Cloud-native path abstraction)

```rust
use object_store::path::Path;

// Platform-agnostic paths
let path = Path::from("data/year=2024/month=01/file.parquet");

// Works identically on:
// - S3: s3://bucket/data/year=2024/month=01/file.parquet
// - Local: /base/data/year=2024/month=01/file.parquet
// - Azure: https://account.blob.core.windows.net/container/data/year=2024/month=01/file.parquet
```

**Why not `std::path::PathBuf`?**
- Object stores use `/` separator (not `\` on Windows)
- No concept of "current directory" or relative paths
- Case-sensitive on all platforms
- UTF-8 enforced (not platform-dependent)

#### **PutPayload** (Flexible data input)

```rust
use bytes::Bytes;
use object_store::PutPayload;

// From static bytes
let payload = PutPayload::from_static(b"hello world");

// From owned bytes
let bytes = Bytes::from(vec![1, 2, 3, 4]);
let payload = PutPayload::from(bytes);

// From file (async read)
let file = tokio::fs::File::open("data.bin").await?;
let payload = PutPayload::from_stream(file);
```

#### **GetResult** (Flexible data output)

```rust
let result: GetResult = store.get(&path).await?;

// Option 1: Buffer entire object in memory
let bytes: Bytes = result.bytes().await?;

// Option 2: Stream the data
let mut stream = result.into_stream();
while let Some(chunk) = stream.next().await {
    let bytes = chunk?;
    // Process chunk
}

// Access metadata
println!("Size: {}", result.meta.size);
println!("Last modified: {}", result.meta.last_modified);
println!("ETag: {:?}", result.meta.e_tag);
```

---

## How It Works: Code Flow

Let's trace a complete **PUT** and **GET** operation through the codebase.

### Example: Writing a File to S3

```rust
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use object_store::aws::AmazonS3Builder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create an S3 backend
    let s3 = AmazonS3Builder::from_env()
        .with_bucket_name("my-bucket")
        .build()?;

    // 2. Define the path
    let path = Path::from("data/file.txt");

    // 3. Write the data
    let data = b"Hello, World!";
    let result = s3.put(&path, data.to_vec().into()).await?;

    println!("Wrote object with ETag: {:?}", result.e_tag);
    Ok(())
}
```

### Code Flow Breakdown

#### Step 1: Builder Pattern

```
User Code
    │
    ├─> AmazonS3Builder::from_env()
    │   └─> Reads AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, etc.
    │
    ├─> .with_bucket_name("my-bucket")
    │   └─> Sets bucket name in builder
    │
    └─> .build()?
        │
        ├─> Validates configuration
        ├─> Creates internal reqwest HTTP client
        ├─> Constructs AmazonS3 struct
        └─> Returns ObjectStore implementation

File: src/aws/builder.rs
```

#### Step 2: Put Operation

```
s3.put(&path, payload)
    │
    ├─> ObjectStoreExt::put() (default implementation)
    │   └─> Calls ObjectStore::put_opts() with default PutOptions
    │
    └─> AmazonS3::put_opts()
        │
        ├─> Converts Path to S3 key
        ├─> Serializes payload to bytes
        ├─> Creates HTTP PUT request
        │   ├─> URL: https://my-bucket.s3.amazonaws.com/data/file.txt
        │   ├─> Headers: Authorization (AWS Signature V4), Content-Type, etc.
        │   └─> Body: payload bytes
        │
        ├─> Sends request via reqwest client
        ├─> Parses S3 response
        │   ├─> Extracts ETag from response headers
        │   └─> Extracts version ID (if versioning enabled)
        │
        └─> Returns PutResult { e_tag: Some("..."), version: Some("...") }

File: src/aws/client.rs
```

### Example: Reading a File from Azure

```rust
use object_store::azure::MicrosoftAzureBuilder;

let azure = MicrosoftAzureBuilder::from_env()
    .with_container_name("my-container")
    .build()?;

let path = Path::from("data/file.txt");
let result = azure.get(&path).await?;
let bytes = result.bytes().await?;

println!("Read {} bytes", bytes.len());
```

#### Code Flow for GET

```
azure.get(&path)
    │
    ├─> ObjectStoreExt::get() (default implementation)
    │   └─> Calls ObjectStore::get_opts() with default GetOptions
    │
    └─> MicrosoftAzure::get_opts()
        │
        ├─> Converts Path to Azure blob name
        ├─> Creates HTTP GET request
        │   ├─> URL: https://account.blob.core.windows.net/container/data/file.txt
        │   ├─> Headers: Authorization (Azure Shared Key), x-ms-version, etc.
        │   └─> Range header (if range specified in GetOptions)
        │
        ├─> Sends request via reqwest client
        ├─> Streams response body
        │   └─> Returns GetResultPayload::Stream (lazy, doesn't buffer yet)
        │
        └─> Returns GetResult {
                payload: Stream,
                meta: ObjectMeta { size, last_modified, e_tag, ... },
                range: 0..file_size,
                attributes: { content_type, ... }
            }

result.bytes().await
    │
    ├─> Consumes the stream
    ├─> Buffers all chunks into a single Bytes
    └─> Returns Bytes

File: src/azure/client.rs
```

### Multipart Upload Flow

For large files (>5MB), use multipart uploads:

```rust
let mut upload = store.put_multipart(&path).await?;

// Upload parts (can be done in parallel)
let part1 = upload.put_part(chunk1.into());
let part2 = upload.put_part(chunk2.into());
let part3 = upload.put_part(chunk3.into());

// Wait for all parts to upload
futures::try_join!(part1, part2, part3)?;

// Atomically complete the upload
upload.complete().await?;
```

#### Multipart Code Flow (S3 Example)

```
store.put_multipart(&path)
    │
    └─> AmazonS3::put_multipart_opts()
        │
        ├─> Creates S3 "CreateMultipartUpload" request
        ├─> S3 returns upload_id: "abc123..."
        └─> Returns S3MultipartUpload { upload_id, path, parts: Vec::new() }

upload.put_part(chunk1)
    │
    └─> S3MultipartUpload::put_part()
        │
        ├─> Increments part number: part_num = 1
        ├─> Spawns async task:
        │   ├─> Creates "UploadPart" request
        │   ├─> URL: /path?uploadId=abc123&partNumber=1
        │   ├─> Body: chunk1 bytes
        │   ├─> Sends request
        │   └─> Stores ETag in parts vector: parts[0] = { num: 1, etag: "xyz" }
        │
        └─> Returns BoxFuture (can be polled later)

upload.complete()
    │
    └─> S3MultipartUpload::complete()
        │
        ├─> Creates "CompleteMultipartUpload" request
        ├─> Body: XML list of all parts with ETags
        │   <CompleteMultipartUpload>
        │     <Part><PartNumber>1</PartNumber><ETag>xyz</ETag></Part>
        │     <Part><PartNumber>2</PartNumber><ETag>abc</ETag></Part>
        │   </CompleteMultipartUpload>
        ├─> S3 atomically assembles all parts into final object
        └─> Returns PutResult

File: src/aws/client.rs (multipart methods)
```

---

## Available Backends

### Feature Flags

Each backend is gated behind a feature flag to minimize dependencies:

```toml
[dependencies]
object_store = { version = "0.13", features = ["aws", "azure", "gcp"] }
```

### 1. **Memory** (Default, no feature required)

```rust
use object_store::memory::InMemory;

let store = InMemory::new();
```

**Use Cases**:
- Unit tests
- Caching layer
- Development without external dependencies

**Implementation**: Uses `Arc<RwLock<HashMap<Path, Bytes>>>` internally.

**File**: `src/memory.rs`

---

### 2. **Local Filesystem** (feature = "fs")

```rust
use object_store::local::LocalFileSystem;

let store = LocalFileSystem::new_with_prefix("/data")?;
```

**Use Cases**:
- Local development
- Single-machine deployments
- Testing with real files

**Implementation**: Uses `tokio::fs` for async file I/O.

**Multipart Strategy**: Single temporary file with seek operations.

**File**: `src/local.rs`

---

### 3. **AWS S3** (feature = "aws")

```rust
use object_store::aws::AmazonS3Builder;

let s3 = AmazonS3Builder::from_env()
    .with_bucket_name("my-bucket")
    .with_region("us-east-1")
    .build()?;
```

**Configuration**:
- Environment: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
- Instance profile (EC2)
- ECS task role
- IMDSv2

**Special Features**:
- Conditional puts (via `PutMode::Create`, `PutMode::Update`)
- Object versioning
- S3 Select (query objects with SQL)
- Native multipart upload API

**File**: `src/aws/`

---

### 4. **Azure Blob Storage** (feature = "azure")

```rust
use object_store::azure::MicrosoftAzureBuilder;

let azure = MicrosoftAzureBuilder::from_env()
    .with_container_name("my-container")
    .with_account("mystorageaccount")
    .build()?;
```

**Authentication**:
- Shared key
- Shared Access Signature (SAS)
- Azure AD (OAuth2)
- Managed Identity

**File**: `src/azure/`

---

### 5. **Google Cloud Storage** (feature = "gcp")

```rust
use object_store::gcp::GoogleCloudStorageBuilder;

let gcs = GoogleCloudStorageBuilder::from_env()
    .with_bucket_name("my-bucket")
    .build()?;
```

**Authentication**:
- Service account JSON key
- Application Default Credentials
- GCE metadata server

**File**: `src/gcp/`

---

### 6. **HTTP/WebDAV** (feature = "http")

```rust
use object_store::http::HttpBuilder;

let http = HttpBuilder::new()
    .with_url("https://example.com/data")
    .build()?;
```

**Limitations**:
- Read-only (GET only)
- No multipart upload
- No listing

**File**: `src/http/`

---

### 7. **Ceph RADOS** (feature = "rados") ⚠️ **New Backend**

```rust
use object_store::rados::RadosBuilder;

let rados = RadosBuilder::new()
    .with_pool_name("my-pool")
    .with_cluster_name("ceph")
    .with_user_name("client.admin")
    .with_conf_file("/etc/ceph/ceph.conf")
    .with_keyring("/etc/ceph/ceph.client.admin.keyring")
    .build()?;
```

**Status**: Skeleton implementation, needs librados integration.

**Files**:
- `src/rados/mod.rs`
- `src/rados/builder.rs`
- `src/rados/client.rs`
- `src/rados/multipart.rs`

---

## Getting Started: Using object_store

### Installation

```toml
[dependencies]
object_store = { version = "0.13", features = ["aws", "fs"] }
tokio = { version = "1", features = ["full"] }
bytes = "1"
futures = "0.3"
```

### Example 1: Basic PUT and GET

```rust
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use object_store::memory::InMemory;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create in-memory store
    let store = InMemory::new();

    // Write data
    let path = Path::from("hello.txt");
    let data = b"Hello, World!";
    store.put(&path, data.to_vec().into()).await?;

    // Read data
    let result = store.get(&path).await?;
    let bytes = result.bytes().await?;
    println!("{}", String::from_utf8_lossy(&bytes));

    Ok(())
}
```

### Example 2: Listing Objects

```rust
use futures::stream::StreamExt;

let prefix = Path::from("data/");
let mut list_stream = store.list(Some(&prefix));

while let Some(meta) = list_stream.next().await.transpose()? {
    println!("{}: {} bytes", meta.location, meta.size);
}
```

### Example 3: Range Reads

```rust
use object_store::GetRange;

// Read only bytes 100-200
let result = store.get_range(&path, 100..200).await?;
let bytes = result.bytes().await?;
assert_eq!(bytes.len(), 100);
```

### Example 4: Conditional Writes

```rust
use object_store::{PutMode, PutOptions, UpdateVersion};

// Only create if doesn't exist
let opts = PutOptions {
    mode: PutMode::Create,
    ..Default::default()
};
store.put_opts(&path, data.into(), opts).await?;

// Conditional update (compare-and-swap)
let opts = PutOptions {
    mode: PutMode::Update(UpdateVersion {
        e_tag: Some("previous-etag".to_string()),
        version: None,
    }),
    ..Default::default()
};
store.put_opts(&path, new_data.into(), opts).await?;
```

### Example 5: Multipart Upload

```rust
// For large files
let mut upload = store.put_multipart(&path).await?;

// Read file in 10MB chunks
let file = tokio::fs::File::open("large_file.bin").await?;
let mut reader = tokio::io::BufReader::new(file);
let mut buffer = vec![0u8; 10 * 1024 * 1024];

loop {
    let n = reader.read(&mut buffer).await?;
    if n == 0 { break; }

    upload.put_part(buffer[..n].to_vec().into()).await?;
}

upload.complete().await?;
```

---

## Advanced Features

### 1. **Vectored I/O** (Read multiple ranges at once)

```rust
let ranges = vec![0..100, 500..600, 1000..1100];
let results = store.get_ranges(&path, &ranges).await?;

for (i, bytes) in results.into_iter().enumerate() {
    println!("Range {}: {} bytes", i, bytes.len());
}
```

**Why?** Efficient for reading Parquet file footers, Apache ORC stripes, etc.

### 2. **Bulk Delete**

```rust
use futures::stream;

let paths_to_delete = vec![
    Path::from("file1.txt"),
    Path::from("file2.txt"),
    Path::from("file3.txt"),
];

let delete_stream = stream::iter(paths_to_delete)
    .map(Ok);

let result_stream = store.delete_stream(delete_stream.boxed());

// Process results
let deleted: Vec<_> = result_stream.try_collect().await?;
println!("Deleted {} objects", deleted.len());
```

### 3. **Copy Operations**

```rust
use object_store::CopyOptions;

let from = Path::from("source.txt");
let to = Path::from("destination.txt");

// Copy object (server-side if possible)
store.copy_opts(&from, &to, CopyOptions::default()).await?;

// Rename (copy + delete)
store.rename_if_not_exists(&from, &to).await?;
```

### 4. **Attributes** (Metadata)

```rust
use object_store::Attributes;

let mut attributes = Attributes::new();
attributes.insert("content-type", "application/json");
attributes.insert("cache-control", "max-age=3600");

let opts = PutOptions {
    attributes,
    ..Default::default()
};

store.put_opts(&path, data.into(), opts).await?;
```

### 5. **Throttling**

```rust
use object_store::throttle::ThrottleConfig;

let config = ThrottleConfig {
    wait_delete_per_call: std::time::Duration::from_millis(100),
    wait_get_per_call: std::time::Duration::from_millis(50),
    wait_list_per_call: std::time::Duration::from_millis(200),
    ..Default::default()
};

let throttled = config.throttle(store);
```

---

## How to Add a New Backend

Let's walk through adding a new backend (e.g., MinIO, which is S3-compatible but might have custom requirements).

### Step 1: Create Module Structure

```
src/
  minio/
    mod.rs       - Main module, CephRados struct, ObjectStore impl
    builder.rs   - MinioBuilder for configuration
    client.rs    - MinioClient with actual I/O logic
    multipart.rs - MinioMultipartUpload implementation
```

### Step 2: Define the Builder

```rust
// src/minio/builder.rs

use crate::{Result, Error};
use std::sync::Arc;
use super::client::MinioClient;
use super::CephMinio;

#[derive(Debug, Clone)]
pub struct MinioBuilder {
    endpoint: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
}

impl MinioBuilder {
    pub fn new() -> Self {
        Self {
            endpoint: None,
            access_key: None,
            secret_key: None,
            bucket: None,
            region: None,
        }
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    pub fn with_access_key(mut self, key: impl Into<String>) -> Self {
        self.access_key = Some(key.into());
        self
    }

    pub fn with_secret_key(mut self, key: impl Into<String>) -> Self {
        self.secret_key = Some(key.into());
        self
    }

    pub fn with_bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }

    pub fn build(self) -> Result<CephMinio> {
        // Validate required fields
        let endpoint = self.endpoint.ok_or_else(|| Error::Generic {
            store: "MinIO",
            source: "endpoint is required".into(),
        })?;

        let bucket = self.bucket.ok_or_else(|| Error::Generic {
            store: "MinIO",
            source: "bucket is required".into(),
        })?;

        let client = MinioClient::new(
            endpoint,
            self.access_key,
            self.secret_key,
            bucket,
            self.region,
        )?;

        Ok(CephMinio::new(Arc::new(client)))
    }
}
```

### Step 3: Implement the Main Struct

```rust
// src/minio/mod.rs

use async_trait::async_trait;
use futures::stream::BoxStream;
use std::sync::Arc;

use crate::{
    GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore, Path,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    MultipartUpload,
};

mod builder;
mod client;
mod multipart;

pub use builder::MinioBuilder;
use client::MinioClient;
use multipart::MinioMultipartUpload;

const STORE: &str = "MinIO";

/// MinIO object storage backend
#[derive(Debug, Clone)]
pub struct CephMinio {
    client: Arc<MinioClient>,
}

impl std::fmt::Display for CephMinio {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MinIO(bucket: {})", self.client.bucket())
    }
}

impl CephMinio {
    pub(crate) fn new(client: Arc<MinioClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ObjectStore for CephMinio {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.client.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.client.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.client.get_opts(location, options).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.client.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.client.list_with_delimiter(prefix).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.client.delete_stream(locations)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> Result<()> {
        self.client.copy_opts(from, to, opts).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.client.rename_if_not_exists(from, to).await
    }
}
```

### Step 4: Implement Client Logic

```rust
// src/minio/client.rs

use bytes::Bytes;
use reqwest::Client;
use crate::{Result, Error, PutPayload, PutOptions, PutResult};

#[derive(Debug)]
pub struct MinioClient {
    endpoint: String,
    bucket: String,
    http_client: Client,
    // ... authentication fields
}

impl MinioClient {
    pub fn new(
        endpoint: String,
        access_key: Option<String>,
        secret_key: Option<String>,
        bucket: String,
        region: Option<String>,
    ) -> Result<Self> {
        let http_client = Client::builder()
            .build()
            .map_err(|e| Error::Generic {
                store: "MinIO",
                source: Box::new(e),
            })?;

        Ok(Self {
            endpoint,
            bucket,
            http_client,
            // ... store auth fields
        })
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        // 1. Construct URL: http://minio:9000/bucket/path
        let url = format!("{}/{}/{}", self.endpoint, self.bucket, location.as_ref());

        // 2. Collect payload bytes
        let data: Bytes = payload.into_bytes(); // You'd implement this

        // 3. Build HTTP request
        let mut request = self.http_client.put(&url);

        // 4. Add authentication headers (AWS Signature V4)
        // ... implement signing logic

        // 5. Send request
        let response = request.body(data).send().await.map_err(|e| Error::Generic {
            store: "MinIO",
            source: Box::new(e),
        })?;

        // 6. Parse response
        let etag = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        Ok(PutResult {
            e_tag: etag,
            version: None,
        })
    }

    // Implement other methods: get_opts, delete_stream, list, etc.
}
```

### Step 5: Implement Multipart Upload

```rust
// src/minio/multipart.rs

use async_trait::async_trait;
use crate::{MultipartUpload, PutPayload, PutResult, Result};

#[derive(Debug)]
pub struct MinioMultipartUpload {
    upload_id: String,
    location: Path,
    // ... client reference, part tracking
}

#[async_trait]
impl MultipartUpload for MinioMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        // Return a future that uploads this part
        Box::pin(async move {
            // Upload part to MinIO
            // Store part ETag
            Ok(())
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        // Send CompleteMultipartUpload request
        Ok(PutResult { e_tag: None, version: None })
    }

    async fn abort(&mut self) -> Result<()> {
        // Send AbortMultipartUpload request
        Ok(())
    }
}
```

### Step 6: Add Feature Flag

```toml
# Cargo.toml

[dependencies]
# ... existing deps

[features]
minio = ["cloud"]  # Reuse cloud dependencies
```

### Step 7: Register in lib.rs

```rust
// src/lib.rs

#[cfg(feature = "minio")]
pub mod minio;

#[cfg_attr(
    feature = "minio",
    doc = "* [`minio`]: [MinIO](https://min.io/). See [`MinioBuilder`](minio::MinioBuilder)"
)]
```

### Step 8: Write Tests

```rust
// src/minio/tests.rs

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder() {
        let builder = MinioBuilder::new()
            .with_endpoint("http://localhost:9000")
            .with_bucket("test")
            .with_access_key("minioadmin")
            .with_secret_key("minioadmin");

        let store = builder.build().unwrap();
        assert_eq!(store.to_string(), "MinIO(bucket: test)");
    }

    #[tokio::test]
    async fn test_put_get() {
        let store = MinioBuilder::new()
            .with_endpoint("http://localhost:9000")
            .with_bucket("test")
            .build()
            .unwrap();

        let path = Path::from("test.txt");
        let data = b"Hello, MinIO!";

        store.put(&path, data.to_vec().into()).await.unwrap();

        let result = store.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();

        assert_eq!(bytes.as_ref(), data);
    }
}
```

---

## Multipart Upload Deep Dive

### Why Multipart?

- **Large files**: Single HTTP requests have limits (typically 5GB)
- **Resumable**: If one part fails, retry only that part
- **Parallel**: Upload multiple parts concurrently
- **Bandwidth**: Better network utilization

### How Different Backends Handle It

| Backend | Strategy | Details |
|---------|----------|---------|
| **S3** | Native API | `CreateMultipartUpload` → `UploadPart` → `CompleteMultipartUpload` |
| **Azure** | Block Blobs | `PutBlock` → `PutBlockList` |
| **GCS** | Resumable Upload | Single session with chunked uploads |
| **Local** | Temp file + seek | Single file, write parts at offsets, rename on complete |
| **Memory** | Vec accumulation | Store parts in vector, concatenate on complete |
| **RADOS** | Temp objects | Each part = separate object, concatenate on complete |

### RADOS Multipart Implementation (Our New Backend)

Since RADOS lacks native multipart:

```
User uploads 3 parts of a 15MB file:

put_multipart("data.bin")
    ↓
    Generates UUID: "550e8400-..."
    ↓
put_part(5MB chunk 1)
    ↓
    Creates RADOS object: "550e8400-...-part-00000" (5MB)
    ↓
put_part(5MB chunk 2)
    ↓
    Creates RADOS object: "550e8400-...-part-00001" (5MB)
    ↓
put_part(5MB chunk 3)
    ↓
    Creates RADOS object: "550e8400-...-part-00002" (5MB)
    ↓
complete()
    ↓
    Read all parts in order:
        part-00000: 5MB
        part-00001: 5MB
        part-00002: 5MB
    ↓
    Concatenate into final object using rados_append():
        rados_write_full(io, "data.bin", part_0_data, 5MB)
        rados_append(io, "data.bin", part_1_data, 5MB)
        rados_append(io, "data.bin", part_2_data, 5MB)
    ↓
    Delete temporary parts:
        rados_remove(io, "550e8400-...-part-00000")
        rados_remove(io, "550e8400-...-part-00001")
        rados_remove(io, "550e8400-...-part-00002")
    ↓
    Done! Final object "data.bin" (15MB) is now visible
```

**Trade-offs**:
- ✅ Works with standard RADOS primitives
- ✅ Parts can be uploaded in parallel
- ❌ Temporary storage overhead (2× during upload)
- ❌ Concatenation step on complete (I/O overhead)

---

## Best Practices and Patterns

### 1. **Use Dynamic Dispatch for Flexibility**

```rust
use object_store::ObjectStore;
use std::sync::Arc;

// Accept any ObjectStore implementation
async fn process_data(store: Arc<dyn ObjectStore>, path: &Path) -> Result<()> {
    let data = store.get(path).await?;
    // ... process
    Ok(())
}

// Can pass any backend
let s3 = Arc::new(AmazonS3Builder::from_env().build()?);
let local = Arc::new(LocalFileSystem::new());

process_data(s3.clone(), &path).await?;
process_data(local.clone(), &path).await?;
```

### 2. **Stream Large Files**

```rust
// ❌ BAD: Loads entire file into memory
let bytes = store.get(&path).await?.bytes().await?;

// ✅ GOOD: Process as stream
let mut stream = store.get(&path).await?.into_stream();
while let Some(chunk) = stream.next().await {
    let bytes = chunk?;
    process_chunk(&bytes).await?;
}
```

### 3. **Use Multipart for >5MB Files**

```rust
const MULTIPART_THRESHOLD: usize = 5 * 1024 * 1024; // 5MB

if file_size < MULTIPART_THRESHOLD {
    // Small file: single PUT
    store.put(&path, data.into()).await?;
} else {
    // Large file: multipart upload
    let mut upload = store.put_multipart(&path).await?;
    for chunk in chunks {
        upload.put_part(chunk.into()).await?;
    }
    upload.complete().await?;
}
```

### 4. **Handle Errors Gracefully**

```rust
let mut upload = store.put_multipart(&path).await?;

// Always abort on error to cleanup partial uploads
let result = async {
    upload.put_part(chunk1.into()).await?;
    upload.put_part(chunk2.into()).await?;
    upload.complete().await
}.await;

match result {
    Ok(put_result) => Ok(put_result),
    Err(e) => {
        // Cleanup on error
        upload.abort().await.ok(); // Ignore abort errors
        Err(e)
    }
}
```

### 5. **Leverage Conditional Operations**

```rust
// Implement optimistic locking
loop {
    let meta = store.head(&path).await?;
    let current_etag = meta.e_tag.clone();

    // Read, modify, write back with version check
    let data = store.get(&path).await?.bytes().await?;
    let new_data = modify(data);

    let opts = PutOptions {
        mode: PutMode::Update(UpdateVersion {
            e_tag: current_etag,
            version: None,
        }),
        ..Default::default()
    };

    match store.put_opts(&path, new_data.into(), opts).await {
        Ok(_) => break, // Success
        Err(Error::Precondition { .. }) => continue, // Retry
        Err(e) => return Err(e),
    }
}
```

---

## Testing Strategy

### Unit Tests

Test individual components in isolation:

```rust
#[tokio::test]
async fn test_put_get() {
    let store = InMemory::new();
    let path = Path::from("test.txt");
    let data = b"hello";

    store.put(&path, data.to_vec().into()).await.unwrap();
    let result = store.get(&path).await.unwrap();
    let bytes = result.bytes().await.unwrap();

    assert_eq!(bytes.as_ref(), data);
}
```

### Integration Tests

Test against real backends (requires credentials):

```rust
#[tokio::test]
#[ignore] // Run only when credentials available
async fn test_s3_multipart() {
    let store = AmazonS3Builder::from_env()
        .with_bucket_name("test-bucket")
        .build()
        .unwrap();

    let path = Path::from("multipart-test.bin");
    let mut upload = store.put_multipart(&path).await.unwrap();

    for i in 0..3 {
        let data = vec![i as u8; 5 * 1024 * 1024];
        upload.put_part(data.into()).await.unwrap();
    }

    upload.complete().await.unwrap();

    // Cleanup
    store.delete(&path).await.unwrap();
}
```

### Property-Based Testing

Use [`proptest`](https://github.com/proptest-rs/proptest) for fuzzing:

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_roundtrip(data in prop::collection::vec(any::<u8>(), 0..1000)) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = InMemory::new();
            let path = Path::from("test");

            store.put(&path, data.clone().into()).await.unwrap();
            let result = store.get(&path).await.unwrap();
            let retrieved = result.bytes().await.unwrap();

            prop_assert_eq!(retrieved.as_ref(), &data);
            Ok(())
        })?;
    }
}
```

---

## Common Pitfalls

### 1. **Forgetting to Poll Multipart Futures**

```rust
// ❌ BAD: Futures not polled
let mut upload = store.put_multipart(&path).await?;
let _p1 = upload.put_part(chunk1.into()); // Not awaited!
let _p2 = upload.put_part(chunk2.into()); // Not awaited!
upload.complete().await?; // May fail or behave unexpectedly
```

```rust
// ✅ GOOD: Await all futures
let mut upload = store.put_multipart(&path).await?;
upload.put_part(chunk1.into()).await?;
upload.put_part(chunk2.into()).await?;
upload.complete().await?;
```

### 2. **Not Handling Partial Failures in Multipart**

```rust
// ✅ GOOD: Use try-join for atomic behavior
let p1 = upload.put_part(chunk1.into());
let p2 = upload.put_part(chunk2.into());
futures::try_join!(p1, p2)?; // Fails if any part fails
```

### 3. **Mixing Path Types**

```rust
// ❌ BAD: Using std::path::PathBuf
let path = std::path::PathBuf::from("data/file.txt"); // Won't compile

// ✅ GOOD: Use object_store::path::Path
let path = object_store::path::Path::from("data/file.txt");
```

### 4. **Not Configuring Timeouts**

```rust
// ✅ GOOD: Set request timeouts
let s3 = AmazonS3Builder::from_env()
    .with_bucket_name("my-bucket")
    .with_timeout(std::time::Duration::from_secs(60))
    .build()?;
```

### 5. **Ignoring E-Tags**

```rust
// Use ETags for caching and validation
let meta1 = store.head(&path).await?;
// ... some time passes ...
let meta2 = store.head(&path).await?;

if meta1.e_tag == meta2.e_tag {
    // Object unchanged, use cached version
} else {
    // Object changed, re-fetch
    let data = store.get(&path).await?;
}
```

---

## Project Structure Summary

```
object_store/
├── src/
│   ├── lib.rs                 # Main entry point, ObjectStore trait
│   ├── upload.rs              # MultipartUpload trait
│   ├── path.rs                # Path abstraction (MISSING - likely in lib.rs)
│   ├── client/                # HTTP client utilities
│   ├── aws/                   # Amazon S3 backend
│   │   ├── mod.rs
│   │   ├── builder.rs
│   │   ├── client.rs
│   │   └── credential.rs
│   ├── azure/                 # Azure Blob backend
│   ├── gcp/                   # Google Cloud Storage backend
│   ├── local.rs               # Local filesystem backend
│   ├── memory.rs              # In-memory backend
│   ├── http.rs                # HTTP/WebDAV backend
│   ├── rados/                 # **NEW** Ceph RADOS backend
│   │   ├── mod.rs
│   │   ├── builder.rs
│   │   ├── client.rs
│   │   └── multipart.rs
│   ├── throttle.rs            # Rate limiting
│   ├── limit.rs               # Concurrency limiting
│   └── buffered.rs            # BufReader/BufWriter adapters
├── Cargo.toml
├── README.md
├── RADOS_IMPLEMENTATION.md    # **NEW** RADOS implementation guide
├── RADOS_CREDENTIALS.md       # **NEW** RADOS auth guide
├── RADOS_MULTIPART.md         # **NEW** RADOS multipart details
└── OBJECT_STORE_TRAINING_GUIDE.md  # **THIS FILE**
```

---

## Quick Reference Card

### Common Operations

```rust
// Create store
let store = InMemory::new();

// Put object
store.put(&path, data.into()).await?;

// Get object
let bytes = store.get(&path).await?.bytes().await?;

// List objects
let mut stream = store.list(Some(&prefix));
while let Some(meta) = stream.next().await.transpose()? {
    println!("{}", meta.location);
}

// Delete object
store.delete(&path).await?;

// Head (metadata only)
let meta = store.head(&path).await?;

// Copy object
store.copy(&from, &to).await?;

// Multipart upload
let mut upload = store.put_multipart(&path).await?;
upload.put_part(chunk.into()).await?;
upload.complete().await?;
```

### Error Handling

```rust
use object_store::Error;

match store.get(&path).await {
    Ok(result) => { /* ... */ },
    Err(Error::NotFound { path, source }) => {
        println!("Object not found: {}", path);
    },
    Err(Error::Precondition { path, source }) => {
        println!("Precondition failed for: {}", path);
    },
    Err(e) => {
        eprintln!("Error: {}", e);
    },
}
```

---

## Next Steps for Learning

1. **Read the examples**: Check `examples/` directory for real-world usage
2. **Explore tests**: Look at integration tests in each backend module
3. **Try it yourself**: Build a small app that lists/downloads files from S3
4. **Contribute**: Pick a TODO item in the RADOS backend and implement it!

---

## Resources

- **Crate Documentation**: https://docs.rs/object_store
- **GitHub Repository**: https://github.com/apache/arrow-rs-object-store
- **Apache Arrow**: https://arrow.apache.org/
- **S3 API Reference**: https://docs.aws.amazon.com/s3/
- **Azure Blob API**: https://learn.microsoft.com/en-us/rest/api/storageservices/blob-service-rest-api
- **Ceph RADOS API**: https://docs.ceph.com/en/latest/rados/api/librados/

---

**Questions? Found an issue?**

Open an issue at: https://github.com/apache/arrow-rs-object-store/issues

**Happy coding! 🚀**
