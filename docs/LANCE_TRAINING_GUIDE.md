# Lance Columnar Format - Training Guide

**A Comprehensive Onboarding Guide for New Developers**

---

## Table of Contents

1. [What is Lance?](#what-is-lance)
2. [Why Lance Exists](#why-lance-exists)
3. [Core Concepts & Architecture](#core-concepts--architecture)
4. [Project Structure](#project-structure)
5. [How It Works: Code Flow](#how-it-works-code-flow)
6. [Key Features Deep Dive](#key-features-deep-dive)
7. [Getting Started: Development Setup](#getting-started-development-setup)
8. [Common Operations](#common-operations)
9. [Advanced Topics](#advanced-topics)
10. [Testing Strategy](#testing-strategy)
11. [Performance Optimization](#performance-optimization)
12. [Contributing Guidelines](#contributing-guidelines)

---

## What is Lance?

**Lance** is a modern **columnar data format** optimized for **machine learning and AI workloads**, particularly multimodal data (images, videos, text, embeddings).

### Key Characteristics

- **Arrow-native**: Built on Apache Arrow for zero-copy interoperability
- **100x faster random access** than Parquet for ML workloads
- **Automatic versioning**: Every write creates a new immutable version
- **Multimodal support**: Efficiently stores images, videos, audio, text, and embeddings
- **Vector search**: Native support for similarity search with various index types
- **Full-text search**: BM25-based text search capabilities
- **Lakehouse format**: Complete lakehouse on object storage (S3, Azure, GCS)

### What It's NOT

- ❌ **Not a database**: It's a file format (like Parquet), though LanceDB builds a database on top
- ❌ **Not just for vectors**: Supports all Arrow data types + multimodal blobs
- ❌ **Not row-oriented**: Columnar format optimized for analytics and ML
- ❌ **Not a query engine**: Though it integrates with DataFusion for SQL queries

---

## Why Lance Exists

### The ML Data Problem

Traditional data formats fall short for modern ML/AI workflows:

| Challenge | Parquet/Iceberg | Lance |
|-----------|----------------|--------|
| **Random Access** | Slow (scan required) | 100x faster (indexed) |
| **Vector Search** | Not supported | Native ANN search |
| **Multimodal Data** | Poor (base64 blobs) | Efficient blob encoding |
| **Versioning** | External (Delta/Iceberg) | Built-in, zero-copy |
| **Schema Evolution** | Full rewrites | Add columns with backfill |
| **Full-Text Search** | Not supported | BM25 inverted index |

### The ML Development Lifecycle

```
Data Collection → Exploration → Feature Engineering → Training → Evaluation → Deployment
       ↑                                                               ↓
       └───────────────────── Monitoring ──────────────────────────────┘
```

**Lance is optimized for EVERY stage**:
- **Collection**: Efficient multimodal blob storage
- **Exploration**: Fast random access for interactive notebooks
- **Analytics**: Columnar format for aggregations
- **Feature Engineering**: Schema evolution without rewrites
- **Training**: High-throughput sequential reads
- **Monitoring**: Time travel and versioning

---

## Core Concepts & Architecture

### 1. File Format Layers

```
┌─────────────────────────────────────────────────────────────┐
│                     Lance Dataset                            │
│  (Table-level abstraction with ACID transactions)           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Manifest System                          │
│  (Version tracking, metadata, transaction log)              │
│  - manifest.json: Current dataset metadata                  │
│  - _versions/*.manifest: Per-version metadata               │
│  - _transactions/*.txn: Transaction logs                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Data Files (.lance)                      │
│  (Columnar storage with custom encodings)                   │
│  - Column chunks with various encodings                     │
│  - Page-level organization for random access                │
│  - Embedded indices for fast lookups                        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Secondary Indices                          │
│  - Vector indices: IVF-PQ, HNSW, etc.                       │
│  - Scalar indices: BTree, Bitmap, Labeled                   │
│  - Full-text indices: Inverted index with BM25              │
└─────────────────────────────────────────────────────────────┘
```

### 2. Key Components

#### **Dataset** (`rust/lance/src/dataset/`)

The top-level abstraction representing a Lance table.

```rust
use lance::Dataset;

// Open existing dataset
let dataset = Dataset::open("s3://bucket/my-dataset").await?;

// Get current version
let version = dataset.version().version;

// Access specific version (time travel)
let old = Dataset::open_version("s3://bucket/my-dataset", 5).await?;
```

**Responsibilities**:
- Schema management
- Version control
- Transaction coordination
- Query planning and execution

#### **Manifest** (`rust/lance/src/dataset/manifest.rs`)

Metadata tracking which data files belong to which version.

```json
{
  "version": 42,
  "schema": {...},
  "fragments": [
    {"id": 0, "files": ["data/0.lance"], "deletion_file": null},
    {"id": 1, "files": ["data/1.lance"], "deletion_file": "deletions/1.arrow"}
  ],
  "index_metadata": {...}
}
```

**Key Properties**:
- **Immutable**: Each version gets its own manifest
- **Incremental**: Only changed fragments listed
- **ACID**: Atomic manifest updates via rename

#### **Fragment** (`rust/lance/src/dataset/fragment.rs`)

A horizontal partition of the dataset (subset of rows).

```
Dataset (1M rows)
├── Fragment 0 (rows 0-250k)     → data/fragment-0.lance
├── Fragment 1 (rows 250k-500k)  → data/fragment-1.lance
├── Fragment 2 (rows 500k-750k)  → data/fragment-2.lance
└── Fragment 3 (rows 750k-1M)    → data/fragment-3.lance
```

**Benefits**:
- Parallel processing (scan multiple fragments concurrently)
- Incremental updates (only rewrite modified fragments)
- Deletion files (soft deletes without rewriting data)

#### **Encoding** (`rust/lance-encoding/`)

Custom encodings optimized for ML data patterns.

| Encoding | Use Case | Compression Ratio |
|----------|----------|------------------|
| **Plain** | Small arrays, already compressed | 1x |
| **Dictionary** | Categorical data, repeated values | 10-100x |
| **RLE** | Sequences of repeated values | 100-1000x |
| **Bitpacking** | Small integers (0-255) | 2-8x |
| **FSST** | String compression | 2-4x |
| **Miniblock** | Nested data, variable-length | 2-10x |
| **Packed Struct** | Structs with nulls | 1.5-3x |

#### **Indices** (`rust/lance-index/`)

Accelerate queries beyond full scans.

**Vector Indices** (for ANN search):
```rust
// IVF-PQ: Inverted File with Product Quantization
dataset.create_index(
    "embeddings",
    IndexType::IVF_PQ,
    Some(&IndexParams::default()
        .with_partitions(256)
        .with_subvectors(16))
).await?;

// Search with index
let results = dataset
    .scan()
    .nearest("embeddings", &query_vector, 10)?
    .try_into_stream().await?;
```

**Scalar Indices** (for filtering):
```rust
// BTree index for range queries
dataset.create_index("timestamp", IndexType::BTree, None).await?;

// Bitmap index for low-cardinality columns
dataset.create_index("category", IndexType::Bitmap, None).await?;

// Full-text search
dataset.create_index("text", IndexType::Inverted, None).await?;
```

### 3. File Format Structure

A Lance data file (`.lance`) contains:

```
┌─────────────────────────────────────────────────────────┐
│                     File Header                          │
│  - Magic number: "LANC"                                 │
│  - Version: 0.1 or 2.0 (v2 is current)                  │
│  - Metadata length                                      │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│                   Column Metadata                        │
│  (Protobuf-encoded schema, statistics, encoding info)   │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│                    Column Chunks                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │  Column 0: "id" (Int64)                         │    │
│  │  - Page 0 (rows 0-1000): Bitpacked encoding    │    │
│  │  - Page 1 (rows 1000-2000): Plain encoding     │    │
│  └─────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────┐    │
│  │  Column 1: "vector" (FixedSizeList<Float32>)   │    │
│  │  - Page 0: Plain encoding                       │    │
│  └─────────────────────────────────────────────────┘    │
│  ┌─────────────────────────────────────────────────┐    │
│  │  Column 2: "image" (Binary blob)                │    │
│  │  - Page 0: Blob encoding (lazy load)            │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│                      Footer                              │
│  - Offset to metadata                                   │
│  - Checksum (optional)                                  │
└─────────────────────────────────────────────────────────┘
```

### 4. Versioning System

Lance uses **Copy-on-Write (CoW)** for zero-copy versioning:

```
Version 1 (initial write)
├── manifest-v1.json
└── data/
    ├── fragment-0.lance (1000 rows)
    └── fragment-1.lance (1000 rows)

Version 2 (append 500 rows)
├── manifest-v2.json
└── data/
    ├── fragment-0.lance (unchanged, reused)
    ├── fragment-1.lance (unchanged, reused)
    └── fragment-2.lance (500 new rows)

Version 3 (delete 100 rows from fragment-0)
├── manifest-v3.json
└── data/
    ├── fragment-0.lance (unchanged)
    ├── fragment-1.lance (unchanged)
    ├── fragment-2.lance (unchanged)
    └── deletions/
        └── fragment-0.arrow (bitmap of deleted rows)

Version 4 (add column "score")
├── manifest-v4.json
└── data/
    ├── fragment-0.lance (rewritten with new column)
    ├── fragment-1.lance (rewritten with new column)
    └── fragment-2.lance (rewritten with new column)
```

**Key Benefits**:
- **Time Travel**: Query any past version
- **Rollback**: Instantly revert to previous version
- **Audit Trail**: Complete history of changes
- **Zero-Copy**: Unchanged fragments shared across versions

---

## Project Structure

### Rust Workspace Layout

```
lance/
├── rust/
│   ├── lance/                    # Main library (file format impl)
│   │   ├── src/
│   │   │   ├── dataset/          # Dataset, scanning, writing
│   │   │   ├── index/            # Index creation and search
│   │   │   ├── io/               # Object store integration
│   │   │   ├── datafusion/       # DataFusion SQL integration
│   │   │   └── blob.rs           # Multimodal blob support
│   │   └── Cargo.toml
│   │
│   ├── lance-core/               # Core types and traits
│   │   ├── src/
│   │   │   ├── datatypes/        # Schema, fields, data types
│   │   │   ├── error.rs          # Error types
│   │   │   └── utils/            # Shared utilities
│   │   └── Cargo.toml
│   │
│   ├── lance-encoding/           # Data encoding/compression
│   │   ├── src/
│   │   │   ├── encodings/        # Encoding implementations
│   │   │   ├── decoder.rs        # Decoding logic
│   │   │   └── encoder.rs        # Encoding logic
│   │   └── Cargo.toml
│   │
│   ├── lance-file/               # Low-level file I/O
│   │   ├── src/
│   │   │   ├── reader.rs         # File reading
│   │   │   ├── writer.rs         # File writing
│   │   │   └── format/           # File format definitions
│   │   └── Cargo.toml
│   │
│   ├── lance-index/              # Indexing algorithms
│   │   ├── src/
│   │   │   ├── vector/           # Vector indices (IVF-PQ, HNSW)
│   │   │   ├── scalar/           # Scalar indices (BTree, Bitmap)
│   │   │   └── optimize/         # Index optimization
│   │   └── Cargo.toml
│   │
│   ├── lance-io/                 # Object store abstraction
│   │   ├── src/
│   │   │   ├── object_store/     # Cloud storage integration
│   │   │   ├── scheduler.rs      # I/O scheduling
│   │   │   └── stream.rs         # Streaming I/O
│   │   └── Cargo.toml
│   │
│   ├── lance-linalg/             # Linear algebra for vectors
│   │   ├── src/
│   │   │   ├── distance/         # Distance metrics (L2, cosine)
│   │   │   ├── kmeans.rs         # K-means clustering
│   │   │   └── matrix.rs         # Matrix operations
│   │   └── Cargo.toml
│   │
│   ├── lance-arrow/              # Arrow integration
│   ├── lance-table/              # Table format
│   ├── lance-datafusion/         # DataFusion SQL engine
│   ├── lance-namespace/          # Catalog abstraction
│   └── compression/              # Compression codecs
│       ├── bitpacking/
│       └── fsst/
│
├── python/                       # Python bindings (PyO3)
│   ├── python/pylance/           # Python wrapper code
│   ├── src/                      # Rust FFI code
│   └── Cargo.toml
│
├── java/                         # Java bindings (JNI)
│   └── core/
│
└── docs/                         # Documentation
    └── src/
```

### Critical Files

| File | Purpose |
|------|---------|
| `rust/lance/src/dataset/write.rs` | Dataset writing and transaction logic |
| `rust/lance/src/dataset/scanner.rs` | Query planning and execution |
| `rust/lance/src/index/vector/ivf.rs` | IVF-PQ vector index |
| `rust/lance-encoding/src/encodings/` | All encoding implementations |
| `rust/lance-file/src/v2/` | Version 2 file format |
| `rust/lance-io/src/object_store.rs` | Cloud storage abstraction |

---

## How It Works: Code Flow

### Example 1: Writing a Dataset

```rust
use lance::Dataset;
use arrow_array::{RecordBatch, Int32Array, FixedSizeListArray};
use arrow_schema::{Schema, Field, DataType};

#[tokio::main]
async fn main() -> lance::Result<()> {
    // 1. Create Arrow RecordBatch
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                128
            ),
            false
        ),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                (0..3).map(|_| Some(vec![Some(0.1); 128])),
                128,
            )),
        ],
    )?;

    // 2. Write dataset
    let uri = "s3://my-bucket/datasets/my-data";
    Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        uri,
        None  // Use default WriteParams
    ).await?;

    Ok(())
}
```

#### Code Flow Breakdown

```
User Code: Dataset::write()
    │
    ├─> Create WriteParams (max_rows_per_file, max_rows_per_group, etc.)
    │
    ├─> Initialize object store (S3/GCS/Azure/local)
    │   └─> lance_io::object_store::ObjectStoreParams
    │
    ├─> Create transaction
    │   ├─> Generate new version number (v1, v2, ...)
    │   └─> Create manifest builder
    │
    ├─> Write data fragments
    │   ├─> For each RecordBatch:
    │   │   ├─> Encode columns with appropriate encoding
    │   │   │   ├─> lance_encoding::encode_batch()
    │   │   │   ├─> Choose encoding based on data type and statistics
    │   │   │   │   ├─> Int32 → Bitpacking or Plain
    │   │   │   │   ├─> String → Dictionary or FSST
    │   │   │   │   └─> FixedSizeList<Float32> → Plain
    │   │   │   └─> Write encoded pages to buffer
    │   │   │
    │   │   ├─> Write column metadata (schema, stats, offsets)
    │   │   └─> Flush to object store: data/fragment-N.lance
    │   │
    │   └─> Add fragment to manifest
    │
    ├─> Finalize transaction
    │   ├─> Write manifest: _versions/N.manifest
    │   ├─> Update latest version pointer: _latest.manifest → _versions/N.manifest
    │   └─> (Atomic rename ensures ACID)
    │
    └─> Return Dataset handle

Files Created:
    my-data/
    ├── _versions/
    │   └── 1.manifest               # Version metadata
    ├── _latest.manifest              # Symlink/pointer to version 1
    └── data/
        ├── fragment-0.lance          # Data file
        └── fragment-1.lance
```

### Example 2: Scanning a Dataset

```rust
use lance::Dataset;
use futures::TryStreamExt;

let dataset = Dataset::open("s3://my-bucket/datasets/my-data").await?;

// Scan with filter and projection
let batches: Vec<RecordBatch> = dataset
    .scan()
    .project(&["id", "vector"])?        // Only read these columns
    .filter("id > 100")?                 // Server-side filtering
    .limit(Some(1000), None)?            // Limit results
    .try_into_stream().await?
    .try_collect().await?;

println!("Read {} batches", batches.len());
```

#### Code Flow

```
dataset.scan()
    │
    ├─> Create ScanBuilder
    │   └─> Load manifest for current version
    │
    ├─> Apply projection (["id", "vector"])
    │   └─> Prune unnecessary columns from scan
    │
    ├─> Apply filter ("id > 100")
    │   ├─> Parse filter expression → DataFusion Expr
    │   ├─> Check if index can accelerate (e.g., BTree on "id")
    │   └─> Plan index-accelerated scan or full scan
    │
    ├─> Apply limit
    │   └─> Short-circuit after 1000 rows
    │
    └─> Execute scan
        │
        ├─> For each fragment in manifest:
        │   ├─> Check fragment statistics (min/max on "id")
        │   │   └─> Skip fragment if all rows filtered out
        │   │
        │   ├─> Open fragment file: data/fragment-N.lance
        │   ├─> Read column metadata
        │   │   └─> Get page offsets for "id" and "vector" columns
        │   │
        │   ├─> Read and decode "id" column
        │   │   ├─> Read encoded pages from file
        │   │   ├─> Decode based on encoding (Bitpacking/Plain/etc.)
        │   │   └─> Materialize as Int32Array
        │   │
        │   ├─> Apply filter on "id" column
        │   │   └─> Create selection vector (bitmap of matching rows)
        │   │
        │   ├─> Read and decode "vector" column (only for selected rows)
        │   │   ├─> Use selection vector to skip non-matching rows
        │   │   └─> Materialize as FixedSizeListArray
        │   │
        │   └─> Yield RecordBatch(id, vector)
        │
        └─> Stream batches to caller
```

### Example 3: Vector Search

```rust
let query = vec![0.1; 128];  // 128-dimensional query vector

let results = dataset
    .scan()
    .nearest("vector", &query, 10)?  // Find 10 nearest neighbors
    .refine(5)?                       // Refine top 5 with exact distance
    .try_into_stream().await?
    .try_collect::<Vec<_>>().await?;
```

#### Code Flow (with IVF-PQ Index)

```
.nearest("vector", &query, 10)
    │
    ├─> Check if vector index exists on "vector" column
    │   └─> Load index metadata from manifest
    │
    ├─> IVF-PQ Index Lookup:
    │   │
    │   ├─> Stage 1: IVF (Inverted File) - Find relevant partitions
    │   │   ├─> Quantize query vector to cluster centroid
    │   │   │   └─> query → nearest centroid (1 of 256 partitions)
    │   │   ├─> Find nprobe closest partitions (e.g., top 10)
    │   │   └─> Load partition data files
    │   │
    │   ├─> Stage 2: PQ (Product Quantization) - Scan partitions
    │   │   ├─> For each partition:
    │   │   │   ├─> Decompose query vector into subvectors (16 chunks of 8D)
    │   │   │   ├─> Compute distance table (16 x 256 distances)
    │   │   │   ├─> Scan PQ codes in partition (each vector → 16 bytes)
    │   │   │   └─> Approximate distance = sum of distances from table
    │   │   │
    │   │   └─> Collect top-K candidates (e.g., 100 approximate matches)
    │   │
    │   └─> Return row IDs of top-K candidates
    │
    ├─> Refine top candidates:
    │   ├─> Read exact vectors for top-K row IDs
    │   ├─> Compute exact L2/cosine distance
    │   └─> Re-rank based on exact distance
    │
    └─> Return final top-10 results with exact distances
```

---

## Key Features Deep Dive

### 1. Multimodal Blob Storage

Lance efficiently stores large binary objects (images, videos, audio) using **blob encoding**:

```rust
use lance::Dataset;
use arrow_array::RecordBatch;

// Store images as blobs
let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int32, false),
    Field::new("image", DataType::Binary, false),  // Raw bytes
]));

let batch = RecordBatch::try_new(
    schema.clone(),
    vec![
        Arc::new(Int32Array::from(vec![1, 2])),
        Arc::new(BinaryArray::from_iter_values(vec![
            include_bytes!("cat.jpg"),
            include_bytes!("dog.jpg"),
        ])),
    ],
)?;

Dataset::write(...).await?;
```

**How it works**:
- Blobs stored in separate "blob files" (not in columnar data)
- Column stores only offsets/references
- Lazy loading: Only fetch blobs when accessed
- Compression: Automatic deduplication and compression

**File layout**:
```
my-dataset/
├── data/
│   ├── fragment-0.lance          # Metadata + small columns
│   └── blobs/
│       ├── blob-0.bin             # Actual blob data
│       └── blob-1.bin
└── _versions/1.manifest
```

### 2. Schema Evolution

Add columns without rewriting existing data:

```rust
let mut dataset = Dataset::open(uri).await?;

// Add new column with default value
dataset
    .add_columns(
        NewColumnTransform::SqlExpressions(vec![
            ("score".to_string(), "random()".to_string())
        ]),
        None
    )
    .await?;
```

**How it works**:
- New column added to schema
- For old fragments: Column computed on-the-fly using expression
- For new fragments: Column physically stored
- No full table rewrite needed

### 3. Deletion Files

Soft deletes without rewriting data:

```rust
dataset.delete("id IN (1, 5, 99)").await?;
```

**File structure**:
```
data/
├── fragment-0.lance               # Original data (unchanged)
└── deletions/
    └── fragment-0_v2.arrow        # Bitmap: [0, 1, 0, 0, 1, ..., 1]
                                   # 1 = deleted, 0 = active
```

**Benefits**:
- Fast deletes (just write bitmap)
- Version control (old versions still accessible)
- Compaction on demand (merge deletes later)

### 4. Full-Text Search

BM25-based full-text search with inverted indices:

```rust
// Create full-text index
dataset
    .create_index("description", IndexType::Inverted, None)
    .await?;

// Search
let results = dataset
    .scan()
    .full_text_search("description", "machine learning")?
    .limit(Some(10), None)?
    .try_into_stream().await?;
```

**Index structure**:
```
Inverted Index:
{
  "machine": [doc1, doc5, doc99],    // Posting lists
  "learning": [doc1, doc5, doc23],
  ...
}

BM25 Scoring:
score(doc, query) = Σ IDF(term) × TF(term, doc) × doc_length_norm
```

### 5. Time Travel Queries

Access any historical version:

```rust
// Current version
let current = Dataset::open(uri).await?;
println!("Version: {}", current.version().version);

// Specific version
let v5 = Dataset::open_version(uri, 5).await?;

// List all versions
let versions = current.versions().await?;
for v in versions {
    println!("v{}: {} fragments, {} rows", v.version, v.fragments.len(), v.row_count);
}
```

---

## Getting Started: Development Setup

### Prerequisites

```bash
# Install Rust (1.75+)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install Python (3.9+)
python3 --version

# Install Java (11+) for Java bindings
java -version
```

### Clone and Build

```bash
# Clone repository
git clone https://github.com/lancedb/lance.git
cd lance

# Build Rust core
cargo build --workspace --release

# Run tests
cargo test --workspace

# Build Python bindings
cd python
pip install maturin
maturin develop

# Run Python tests
cd ..
make test -C python
```

### Project Commands

```bash
# Rust development
cargo check --workspace --tests              # Check for errors
cargo test -p lance-core                     # Test specific crate
cargo clippy --all --tests -- -D warnings    # Lint
cargo fmt --all                              # Format

# Python development (from python/)
maturin develop                              # Build + install
make test                                    # Run tests
make lint                                    # Lint Python code
pytest python/tests/test_dataset.py::test_write  # Single test

# Coverage reports
cargo +nightly llvm-cov -p lance-core --branch --html
```

---

## Common Operations

### Creating a Dataset

```rust
use lance::Dataset;
use arrow_array::*;
use arrow_schema::*;

// From RecordBatch iterator
let batches = /* ... */;
Dataset::write(batches, "data/my-table", None).await?;

// From Parquet
let parquet = ParquetDataset::new("data.parquet")?;
Dataset::write(parquet, "data/lance-table", None).await?;

// With custom parameters
let params = WriteParams {
    max_rows_per_file: 1024 * 1024,
    max_rows_per_group: 1024,
    mode: WriteMode::Append,
    ..Default::default()
};
Dataset::write(batches, uri, Some(params)).await?;
```

### Scanning and Filtering

```rust
// Full scan
let batches = dataset.scan()
    .try_into_stream().await?
    .try_collect::<Vec<_>>().await?;

// Projection + filter
let batches = dataset.scan()
    .project(&["id", "name"])?
    .filter("age >= 18 AND country = 'US'")?
    .try_into_stream().await?;

// With limit
let first_100 = dataset.scan()
    .limit(Some(100), None)?
    .try_into_stream().await?;
```

### Vector Search

```rust
// Create IVF-PQ index
dataset.create_index(
    "embeddings",
    IndexType::IVF_PQ,
    Some(&IndexParams::default()
        .with_partitions(256)      // Number of IVF partitions
        .with_subvectors(16)       // PQ subvectors
        .with_bits(8))             // Bits per subvector
).await?;

// Search
let query_vector = vec![0.1; 768];
let results = dataset.scan()
    .nearest("embeddings", &query_vector, 100)?
    .nprobe(10)?                   // Search 10 partitions
    .refine(20)?                   // Refine top 20 exactly
    .try_into_stream().await?;
```

### Schema Evolution

```rust
// Add column
dataset.add_columns(
    NewColumnTransform::SqlExpressions(vec![
        ("new_col".to_string(), "old_col * 2".to_string())
    ]),
    None
).await?;

// Drop column
dataset.drop_columns(&["old_col"]).await?;

// Alter column (rename)
dataset.alter_columns(&[
    ("old_name".to_string(), "new_name".to_string())
]).await?;
```

### Versioning Operations

```rust
// List versions
let versions = dataset.versions().await?;

// Restore to previous version
dataset.restore(5).await?;

// Cleanup old versions
dataset.cleanup_old_versions(
    Duration::from_secs(7 * 24 * 60 * 60),  // Keep last 7 days
    None
).await?;
```

---

## Advanced Topics

### 1. Custom Encodings

Implement custom encoding for special data types:

```rust
use lance_encoding::{Encoder, Decoder};

struct MyCustomEncoder;

impl Encoder for MyCustomEncoder {
    fn encode(&mut self, data: &dyn Array) -> Result<Vec<u8>> {
        // Custom encoding logic
    }
}
```

### 2. Custom Indices

Create specialized indices for domain-specific queries:

```rust
// Geospatial index
dataset.create_index(
    "location",
    IndexType::Custom("geo_hash"),
    Some(&json!({
        "precision": 6,
        "hash_type": "geohash"
    }))
).await?;
```

### 3. Integration with DataFusion

SQL queries via Apache DataFusion:

```rust
use lance::datafusion::LanceDataFusionExt;
use datafusion::prelude::*;

let ctx = SessionContext::new();
ctx.register_lance("my_table", dataset)?;

let df = ctx.sql("
    SELECT id, vector
    FROM my_table
    WHERE category = 'positive'
    LIMIT 100
").await?;

let batches = df.collect().await?;
```

### 4. Parallel Writes

Concurrent fragment writes for high throughput:

```rust
use lance::dataset::WriteParams;

let params = WriteParams {
    max_rows_per_file: 100_000,
    mode: WriteMode::Append,
    enable_move_stable_row_ids: true,
    ..Default::default()
};

// Multiple writers can append concurrently
let handle1 = tokio::spawn(async { dataset.append(batches1, None).await });
let handle2 = tokio::spawn(async { dataset.append(batches2, None).await });

futures::try_join!(handle1, handle2)?;
```

---

## Testing Strategy

### Unit Tests

Test individual components:

```rust
#[tokio::test]
async fn test_write_read_roundtrip() {
    let uri = "memory://test";

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    ).unwrap();

    Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone()),
        uri,
        None
    ).await.unwrap();

    let dataset = Dataset::open(uri).await.unwrap();
    let result = dataset.scan().try_into_stream().await.unwrap()
        .try_collect::<Vec<_>>().await.unwrap();

    assert_eq!(result[0], batch);
}
```

### Integration Tests

Test against real object stores:

```bash
# Start local S3 (MinIO)
docker run -p 9000:9000 minio/minio server /data

# Run S3 tests
AWS_DEFAULT_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
cargo test --features=integration test_s3
```

### Property-Based Tests

Use `proptest` for fuzz testing:

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_encoding_roundtrip(data in prop::collection::vec(0i32..1000, 0..100)) {
        let array = Int32Array::from(data.clone());
        let encoded = encode(&array)?;
        let decoded = decode(&encoded)?;
        assert_eq!(decoded.as_primitive::<Int32Type>(), &array);
    }
}
```

---

## Performance Optimization

### 1. Encoding Selection

Choose encodings based on data characteristics:

| Data Pattern | Encoding | Example |
|-------------|----------|---------|
| Sorted integers | Bitpacking | Timestamps, IDs |
| Repeated values | RLE | Categories, flags |
| Low cardinality | Dictionary | Countries, statuses |
| Strings | FSST | Text, URLs |
| Already compressed | Plain | JPEGs, PNGs |

### 2. Fragment Sizing

Optimize fragment size for workload:

```rust
let params = WriteParams {
    // Smaller fragments: Better for updates, worse for scans
    max_rows_per_file: 50_000,

    // Larger fragments: Better for scans, worse for updates
    // max_rows_per_file: 1_000_000,

    ..Default::default()
};
```

### 3. Index Tuning

Tune vector indices for recall/latency trade-off:

```rust
// Higher recall (slower)
IndexParams::default()
    .with_partitions(256)
    .with_subvectors(32)       // More subvectors = higher accuracy
    .with_bits(8)

// Lower latency (lower recall)
IndexParams::default()
    .with_partitions(512)      // More partitions = smaller scans
    .with_subvectors(8)        // Fewer subvectors = faster
    .with_bits(4)              // Fewer bits = more compression
```

### 4. I/O Optimization

Use efficient I/O patterns:

```rust
use lance::io::ObjectStoreParams;

let params = ObjectStoreParams {
    // Parallel fragment reads
    max_parallelism: Some(16),

    // Larger buffers for cloud storage
    buffer_size: 8 * 1024 * 1024,  // 8MB

    ..Default::default()
};
```

---

## Contributing Guidelines

### Code Style

```rust
// ✅ Good: Use Into<T> for flexibility
pub fn create_index(name: impl Into<String>, index_type: IndexType) -> Result<()> {
    let name = name.into();
    // ...
}

// ❌ Bad: Forces caller to allocate String
pub fn create_index(name: String, index_type: IndexType) -> Result<()> {
    // ...
}
```

### Documentation

```rust
/// Creates a new vector index on the specified column.
///
/// # Arguments
///
/// * `column` - The name of the column to index (must be FixedSizeList<Float32>)
/// * `index_type` - The type of index to create (IVF_PQ, HNSW, etc.)
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if:
/// - Column doesn't exist
/// - Column is not a vector type
/// - Index already exists
///
/// # Example
///
/// ```
/// # use lance::Dataset;
/// # async fn example(dataset: &Dataset) -> lance::Result<()> {
/// dataset.create_index("embeddings", IndexType::IVF_PQ, None).await?;
/// # Ok(())
/// # }
/// ```
pub async fn create_index(...) -> Result<()> { ... }
```

### Testing Requirements

All PRs must include:
- ✅ Unit tests for new functionality
- ✅ Integration tests for cross-module features
- ✅ Documentation examples that compile
- ✅ Error case coverage

### Submitting Changes

```bash
# 1. Format code
cargo fmt --all

# 2. Run lints
cargo clippy --all --tests -- -D warnings

# 3. Run tests
cargo test --workspace

# 4. Check coverage
cargo +nightly llvm-cov -p <crate> --branch

# 5. Create PR with clear description
```

---

## Resources

- **Documentation**: https://lance.org
- **API Reference**: https://docs.rs/lance
- **GitHub**: https://github.com/lancedb/lance
- **Discord**: https://discord.gg/lance
- **Blog**: https://blog.lancedb.com

---

**Welcome to the Lance community! 🚀**

Start with simple read/write operations, explore the examples, and gradually dive into advanced features like vector search and custom indices. Don't hesitate to ask questions on Discord or open issues on GitHub.

Happy coding!
