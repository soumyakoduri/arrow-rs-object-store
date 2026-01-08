# LanceDB - Training Guide

**A Comprehensive Onboarding Guide for New Developers**

---

## Table of Contents

1. [What is LanceDB?](#what-is-lancedb)
2. [Why LanceDB Exists](#why-lancedb-exists)
3. [Core Architecture](#core-architecture)
4. [Project Structure](#project-structure)
5. [How It Works: Code Flow](#how-it-works-code-flow)
6. [Getting Started: Development](#getting-started-development)
7. [Key Features Deep Dive](#key-features-deep-dive)
8. [API Design Patterns](#api-design-patterns)
9. [Embedding Integrations](#embedding-integrations)
10. [Remote vs Local Architecture](#remote-vs-local-architecture)
11. [Adding New Features](#adding-new-features)
12. [Best Practices](#best-practices)

---

## What is LanceDB?

**LanceDB** is a **multimodal vector database** built on top of the Lance columnar format. It provides a high-level API for vector search, full-text search, and hybrid queries.

### Key Characteristics

- **Embedded database**: Runs in-process like SQLite (local mode)
- **Cloud-native**: Optional remote mode connects to LanceDB Cloud
- **Multimodal**: Store and search vectors, text, images, videos, and more
- **Hybrid search**: Combines vector similarity, full-text (BM25), and SQL
- **Zero-config**: No servers to manage in local mode
- **Multi-language**: Native Rust, Python, TypeScript/JavaScript, and Java SDKs

### What It's NOT

- ❌ **Not just a vector database**: Full SQL support, not limited to embeddings
- ❌ **Not a graph database**: Optimized for vector/columnar data
- ❌ **Not a traditional RDBMS**: No strong ACID guarantees across distributed queries
- ❌ **Not a replacement for OLTP databases**: Optimized for ML/AI workloads

---

## Why LanceDB Exists

### The Vector Database Landscape (2024)

| Database | Architecture | Best For |
|----------|-------------|----------|
| **Pinecone** | Cloud-only, managed | Simple cloud deployments |
| **Weaviate** | Server-based | Kubernetes deployments |
| **Qdrant** | Server-based | High-performance cloud |
| **Chroma** | Embedded | Local development |
| **Milvus** | Distributed server | Large-scale production |
| **LanceDB** | **Embedded + Cloud** | **Local dev → Cloud prod seamlessly** |

### LanceDB's Unique Value

```
┌─────────────────────────────────────────────────────────────┐
│                    Development Workflow                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Local Development (LanceDB Embedded)                       │
│  ├── No servers, no Docker, no setup                        │
│  ├── Fast iteration on laptop                               │
│  └── All features work offline                              │
│                                                              │
│                         ↓                                    │
│              (Change 1 line of code)                         │
│                         ↓                                    │
│                                                              │
│  Production (LanceDB Cloud)                                  │
│  ├── Same API, same code                                    │
│  ├── Scalable, managed infrastructure                       │
│  └── Multi-tenancy, authentication, etc.                    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

**The switch**:
```python
# Local development
db = lancedb.connect("./my-data")

# Production (change URI only!)
db = lancedb.connect("db://prod-instance")
```

---

## Core Architecture

### 1. Architectural Layers

```
┌─────────────────────────────────────────────────────────────┐
│                   LanceDB API Layer                          │
│  (Python/TypeScript/Rust/Java SDKs)                         │
│  - High-level database operations                           │
│  - Query builders and execution                             │
│  - Embedding integrations                                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│               LanceDB Core (Rust)                            │
│  - Connection management                                    │
│  - Table operations (CRUD)                                  │
│  - Query planning and optimization                          │
│  - Index management                                         │
│  - Embedding model integrations                             │
└─────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    ▼                   ▼
        ┌───────────────────┐  ┌────────────────────┐
        │  Local Backend    │  │  Remote Backend    │
        │  (Lance Format)   │  │  (HTTP API)        │
        └───────────────────┘  └────────────────────┘
                    │                   │
                    ▼                   ▼
        ┌───────────────────┐  ┌────────────────────┐
        │  Object Store     │  │  LanceDB Cloud     │
        │  (S3/GCS/Azure/   │  │  (Managed Service) │
        │   Local FS)       │  │                    │
        └───────────────────┘  └────────────────────┘
```

### 2. Core Components

#### **Database** (`rust/lancedb/src/database.rs`)

The main entry point representing a connection to a LanceDB instance.

```rust
pub struct Database {
    uri: String,
    backend: Arc<dyn DatabaseBackend>,  // NativeDatabase or RemoteDatabase
    // ...
}

#[async_trait]
pub trait DatabaseBackend: Send + Sync {
    async fn create_table(&self, name: &str, batches: Box<dyn RecordBatchReader>) -> Result<Table>;
    async fn open_table(&self, name: &str) -> Result<Table>;
    async fn list_tables(&self) -> Result<Vec<String>>;
    async fn drop_table(&self, name: &str) -> Result<()>;
}
```

**Two implementations**:
1. **NativeDatabase**: Local in-process database using Lance format
2. **RemoteDatabase**: HTTP client to LanceDB Cloud

#### **Table** (`rust/lancedb/src/table.rs`)

Represents a single table within a database.

```rust
pub struct Table {
    name: String,
    inner: Arc<dyn BaseTable>,  // NativeTable or RemoteTable
}

#[async_trait]
pub trait BaseTable: Send + Sync {
    async fn schema(&self) -> Result<Schema>;
    async fn count_rows(&self) -> Result<usize>;
    async fn add(&mut self, batches: Box<dyn RecordBatchReader>) -> Result<()>;
    async fn delete(&mut self, predicate: &str) -> Result<()>;
    async fn update(&mut self, updates: HashMap<String, String>) -> Result<()>;
    async fn create_index(&self, columns: &[String], index: Index) -> Result<()>;
    // ... vector search, full-text search, etc.
}
```

#### **Query** (`rust/lancedb/src/query.rs`)

Builder pattern for constructing queries.

```rust
pub struct Query {
    table: Arc<dyn BaseTable>,
    vector_query: Option<VectorQuery>,
    filter: Option<String>,
    limit: Option<usize>,
    columns: Option<Vec<String>>,
    full_text_query: Option<FullTextQuery>,
    // ...
}

impl Query {
    pub fn nearest_to(self, vector: &[f32]) -> Result<Self> { ... }
    pub fn where_clause(self, filter: &str) -> Result<Self> { ... }
    pub fn select(self, columns: &[String]) -> Result<Self> { ... }
    pub fn limit(self, n: usize) -> Result<Self> { ... }
    pub async fn execute(self) -> Result<RecordBatchStream> { ... }
}
```

#### **Embeddings** (`rust/lancedb/src/embeddings.rs`)

Pluggable embedding models for automatic vectorization.

```rust
#[async_trait]
pub trait EmbeddingModel: Send + Sync {
    async fn embed_text(&self, text: &str) -> Result<Vec<f32>>;
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;
    fn dimension(&self) -> usize;
}

// Built-in implementations:
// - OpenAIEmbeddings
// - BedrockEmbeddings (AWS)
// - SentenceTransformersEmbeddings (local models)
```

---

## Project Structure

```
lancedb/
├── rust/
│   └── lancedb/
│       ├── src/
│       │   ├── lib.rs                # Main entry point, public API
│       │   ├── connection.rs         # ConnectOptions builder
│       │   ├── database.rs           # Database trait and implementations
│       │   ├── table.rs              # Table trait and NativeTable
│       │   ├── query.rs              # Query builder
│       │   ├── index.rs              # Index types and creation
│       │   ├── embeddings.rs         # Embedding model trait
│       │   ├── io/                   # Object store configuration
│       │   ├── remote/               # Remote client (LanceDB Cloud)
│       │   │   ├── client.rs
│       │   │   ├── table.rs          # RemoteTable implementation
│       │   │   └── database.rs       # RemoteDatabase implementation
│       │   ├── rerankers.rs          # Reranking functionality
│       │   └── dataloader.rs         # Data loading utilities
│       └── Cargo.toml
│
├── python/
│   ├── python/lancedb/             # Python wrapper code
│   │   ├── __init__.py
│   │   ├── db.py                   # AsyncDatabase wrapper
│   │   ├── table.py                # Table classes (AsyncTable, LanceTable)
│   │   ├── query.py                # Query builder
│   │   ├── embeddings/             # Embedding integrations
│   │   │   ├── openai.py
│   │   │   ├── sentence_transformers.py
│   │   │   └── ...
│   │   ├── remote/                 # Remote table wrapper
│   │   └── pydantic.py             # Pydantic model support
│   ├── src/                        # PyO3 Rust bindings
│   │   ├── lib.rs
│   │   ├── database.rs             # Python → Rust FFI
│   │   ├── table.rs
│   │   └── query.rs
│   └── Cargo.toml
│
├── nodejs/
│   ├── lancedb/                    # TypeScript wrapper code
│   │   ├── index.ts
│   │   ├── connection.ts
│   │   ├── table.ts
│   │   └── query.ts
│   ├── src/                        # napi-rs Rust bindings
│   │   ├── lib.rs
│   │   ├── database.rs             # Node.js → Rust FFI
│   │   └── table.rs
│   └── package.json
│
├── java/
│   └── lancedb-core/               # Java SDK (for LanceDB Cloud)
│       └── src/main/java/
│
└── docs/                           # Documentation
    └── src/
```

### Key Files

| File | Purpose |
|------|---------|
| `rust/lancedb/src/lib.rs` | Public Rust API entry point |
| `rust/lancedb/src/table.rs` | NativeTable implementation (local) |
| `rust/lancedb/src/remote/table.rs` | RemoteTable implementation (cloud) |
| `rust/lancedb/src/query.rs` | Query builder and execution |
| `python/python/lancedb/table.py` | Python Table wrappers (sync + async) |
| `nodejs/lancedb/native_table.ts` | TypeScript Table implementation |

---

## How It Works: Code Flow

### Example 1: Creating a Table (Python)

```python
import lancedb
import pandas as pd

# Connect to database
db = lancedb.connect("./my-lancedb")

# Create DataFrame with vectors
df = pd.DataFrame({
    "id": [1, 2, 3],
    "text": ["hello", "world", "lance"],
    "vector": [[0.1, 0.2], [0.3, 0.4], [0.5, 0.6]]
})

# Create table
table = db.create_table("my_table", df)
```

#### Code Flow (Python → Rust → Lance)

```
Python: db.create_table("my_table", df)
    │
    ├─> Convert DataFrame to Arrow RecordBatch
    │   └─> Uses pyarrow.Table.from_pandas()
    │
    ├─> Call PyO3 binding: _lancedb.Database.create_table()
    │
    └─> Rust (python/src/database.rs):
        │
        ├─> PyDatabase::create_table()
        │   ├─> Extract RecordBatchReader from Python
        │   └─> Call core: self.db.create_table(name, batches).await
        │
        └─> Rust Core (rust/lancedb/src/database.rs):
            │
            ├─> Database::create_table()
            │   └─> Delegate to backend: self.backend.create_table()
            │
            └─> NativeDatabase::create_table() (local mode)
                │
                ├─> Validate schema (ensure vector columns are FixedSizeList)
                │
                ├─> Call Lance: lance::Dataset::write()
                │   ├─> Create new dataset at {db_uri}/tables/my_table.lance
                │   ├─> Write RecordBatches to Lance format
                │   └─> Create initial manifest (version 1)
                │
                └─> Return NativeTable handle
                    │
                    └─> Wrap in Python: return PyTable(table)

Files Created on Disk:
    my-lancedb/
    └── tables/
        └── my_table.lance/
            ├── _versions/
            │   └── 1.manifest
            ├── _latest.manifest
            └── data/
                └── fragment-0.lance
```

### Example 2: Vector Search (TypeScript)

```typescript
import * as lancedb from "@lancedb/lancedb";

const db = await lancedb.connect("./my-lancedb");
const table = await db.openTable("my_table");

// Vector search
const results = await table
  .search([0.15, 0.25])  // Query vector
  .limit(10)
  .toArrow();

console.log(results);
```

#### Code Flow (TypeScript → Rust → Lance)

```
TypeScript: table.search([0.15, 0.25]).limit(10).toArrow()
    │
    ├─> Create Query builder: new Query(table, vector)
    │
    ├─> Build query chain:
    │   ├─> query.limit(10)
    │   └─> query.toArrow()
    │
    └─> Call napi-rs binding: table.search(...)
        │
        └─> Rust (nodejs/src/table.rs):
            │
            ├─> Table::search()
            │   ├─> Extract vector from JavaScript Float32Array
            │   └─> Call core: self.table.query()
            │
            └─> Rust Core (rust/lancedb/src/table.rs):
                │
                ├─> NativeTable::query()
                │   └─> Create Query builder
                │
                ├─> Query::nearest_to(vector)
                │   ├─> Store vector query parameters
                │   └─> Set query_type = VectorSearch
                │
                ├─> Query::limit(10)
                │
                └─> Query::execute()
                    │
                    ├─> Build Lance scanner
                    │   └─> lance::Dataset::scan()
                    │
                    ├─> Apply vector search:
                    │   ├─> scanner.nearest("vector", &query_vec, 10)
                    │   │   │
                    │   │   ├─> Check for vector index
                    │   │   │   └─> Load IVF-PQ index if exists
                    │   │   │
                    │   │   ├─> Execute ANN search:
                    │   │   │   ├─> IVF: Find nearest partitions
                    │   │   │   ├─> PQ: Approximate distances
                    │   │   │   └─> Return top-10 row IDs
                    │   │   │
                    │   │   └─> Refine with exact distances
                    │   │
                    │   └─> scanner.try_into_stream()
                    │
                    ├─> Collect RecordBatches
                    │
                    └─> Return to JavaScript as Arrow Table
```

### Example 3: Hybrid Search (Rust)

```rust
use lancedb::{connect, query::{QueryBase, ExecutableQuery}};
use lance_index::scalar::FullTextSearchQuery;

#[tokio::main]
async fn main() -> Result<()> {
    let db = connect("./my-lancedb").execute().await?;
    let table = db.open_table("documents").execute().await?;

    // Hybrid search: vector + full-text + SQL filter
    let results = table
        .query()
        .nearest_to(&[0.1; 768])?           // Vector similarity
        .full_text_search(FullTextSearchQuery::new("machine learning".into()))  // BM25
        .where_clause("published_year >= 2020")?  // SQL filter
        .limit(20)
        .execute()
        .await?;

    // Process results
    while let Some(batch) = results.next().await {
        println!("{:?}", batch?);
    }

    Ok(())
}
```

#### Hybrid Search Execution Flow

```
Query Execution Pipeline:
    │
    ├─> Stage 1: Full-Text Search
    │   ├─> Load inverted index for "text" column
    │   ├─> Search for "machine" → [doc5, doc12, doc99, ...]
    │   ├─> Search for "learning" → [doc5, doc99, doc123, ...]
    │   ├─> Compute BM25 scores
    │   └─> Filter candidates (top 1000 by BM25)
    │
    ├─> Stage 2: SQL Filter
    │   ├─> Apply filter: "published_year >= 2020"
    │   ├─> Check BTree index on "published_year"
    │   └─> Intersect with full-text candidates → [doc5, doc99, ...]
    │
    ├─> Stage 3: Vector Search
    │   ├─> Load vector index (IVF-PQ)
    │   ├─> ANN search within filtered candidates
    │   ├─> Compute exact cosine/L2 distances
    │   └─> Rank by vector similarity
    │
    └─> Stage 4: Final Ranking
        ├─> Combine scores: α·vector_score + β·bm25_score
        ├─> Sort by combined score
        └─> Return top 20 results
```

---

## Getting Started: Development

### Prerequisites

```bash
# Rust (1.75+)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Python (3.9+)
python3 --version

# Node.js (18+)
node --version

# Java (11+) - for Java bindings
java -version
```

### Building from Source

```bash
# Clone repository
git clone https://github.com/lancedb/lancedb.git
cd lancedb

# Build Rust core
cd rust/lancedb
cargo build --release --features remote

# Run Rust tests
cargo test --features remote

# Build Python bindings
cd ../../python
pip install maturin
maturin develop

# Test Python
pytest python/tests/

# Build TypeScript bindings
cd ../nodejs
npm install
npm run build

# Test TypeScript
npm test
```

### Development Commands

#### Rust

```bash
# From rust/lancedb/
cargo check --quiet --features remote --tests
cargo test --quiet --features remote
cargo clippy --quiet --features remote --tests -- -D warnings
cargo fmt --all
```

#### Python

```bash
# From python/
make develop          # Build and install locally
make test            # Run all tests
make lint            # Lint Python code
make typecheck       # MyPy type checking
pytest -vv python/tests/test_table.py::test_basic  # Single test
```

#### TypeScript

```bash
# From nodejs/
npm run build        # Build Rust + TypeScript
npm run lint         # ESLint
npm test            # Run all tests
npm test -- __test__/table.test.ts  # Single test file
npm run docs        # Generate API docs
```

---

## Key Features Deep Dive

### 1. Automatic Embedding Generation

LanceDB can automatically generate embeddings from text/images:

```python
import lancedb
from lancedb.embeddings import get_registry

# Register embedding model
model = get_registry().get("openai").create(name="text-embedding-3-small")

# Create table with auto-embedding
db = lancedb.connect("./my-db")
table = db.create_table(
    "documents",
    data=[
        {"text": "Lance is fast", "id": 1},
        {"text": "LanceDB is great", "id": 2},
    ],
    embedding_functions=[
        model.create_source_field("text").create_vector_field("vector")
    ]
)

# Embeddings automatically generated!
# Search with text (auto-embedded)
results = table.search("fast database").limit(5).to_pandas()
```

**How it works**:
1. User provides text data (no vectors)
2. LanceDB calls embedding API (OpenAI/Bedrock/local model)
3. Vectors added to table automatically
4. Search queries also auto-embedded

### 2. Pydantic Integration

Type-safe table schemas with Pydantic:

```python
from lancedb.pydantic import LanceModel, Vector
from pydantic import BaseModel

class Document(LanceModel):
    id: int
    text: str
    vector: Vector(768)  # 768-dimensional vector
    metadata: dict

# Create table from schema
table = db.create_table("docs", schema=Document)

# Insert with validation
table.add([
    Document(id=1, text="hello", vector=[0.1]*768, metadata={"source": "web"})
])

# Query returns Pydantic models
results = table.search([0.1]*768).limit(5).to_pydantic(Document)
for doc in results:
    assert isinstance(doc, Document)
    print(doc.text)
```

### 3. Full-Text Search (BM25)

Keyword search with BM25 ranking:

```python
# Create full-text index
table.create_fts_index("text", use_tantivy=False)

# Search
results = table.search("machine learning").limit(10).to_pandas()

# Hybrid: vector + full-text
results = (
    table
    .search([0.1]*768, vector_column_name="embedding")
    .where("text LIKE '%AI%'", prefilter=True)
    .limit(10)
    .to_pandas()
)
```

**Index structure**:
```
_indices/
└── text_fts.lance/
    ├── tokens/          # Token → posting list mapping
    ├── statistics/      # Document frequencies, term counts
    └── metadata/        # Index configuration
```

### 4. Reranking

Improve recall with multi-stage retrieval:

```python
from lancedb.rerankers import ColbertReranker

reranker = ColbertReranker()

# Initial retrieval (fast ANN)
results = (
    table
    .search("machine learning")
    .limit(100)             # Recall 100 candidates
    .rerank(reranker=reranker, k=10)  # Rerank to top 10
    .to_pandas()
)
```

**Two-stage pipeline**:
1. **Stage 1 (Fast)**: ANN search returns 100 candidates (~5ms)
2. **Stage 2 (Accurate)**: Rerank with expensive model (ColBERT) → top 10 (~50ms)

### 5. Versioning and Time Travel

Access historical data:

```python
# Current version
table = db.open_table("my_table")
print(table.version)  # e.g., 42

# Time travel
table_v5 = db.open_table("my_table", version=5)

# List all versions
versions = table.list_versions()
for v in versions:
    print(f"v{v['version']}: {v['timestamp']} - {v['metadata']}")

# Restore to previous version
table.restore(version=10)
```

### 6. Incremental Updates

Efficient updates without full table rewrites:

```python
# Add new rows
table.add([{"id": 100, "text": "new doc", "vector": [0.5]*768}])

# Delete rows
table.delete("id = 25")

# Update rows
table.update(where="id = 10", values={"text": "updated text"})

# Merge (upsert)
table.merge_insert("id") \
    .when_matched_update_all() \
    .when_not_matched_insert_all() \
    .execute(new_data)
```

**What happens on disk**:
- **Add**: New fragment created (fast)
- **Delete**: Deletion file created (fast, no rewrite)
- **Update**: Modified rows rewritten to new fragment
- **Compact**: Periodically merge fragments and apply deletes

---

## API Design Patterns

### 1. Builder Pattern

All configuration uses builders for extensibility:

```rust
// Rust
let db = lancedb::connect("./my-db")
    .aws_creds(AwsCredential { ... })
    .read_consistency_interval(Duration::from_secs(5))
    .execute()
    .await?;

let table = db.create_table("tbl")
    .mode(CreateMode::Overwrite)
    .enable_v2_manifest_paths(true)
    .execute(batches)
    .await?;
```

```python
# Python
db = (
    lancedb.connect("./my-db")
    .aws_creds(...)
    .read_consistency_interval(5.0)
    .execute()
)
```

### 2. Async-First API

Python provides both sync and async:

```python
# Async API
import lancedb

db = await lancedb.connect_async("./my-db")
table = await db.open_table("tbl")
results = await table.search([0.1]*768).limit(10).to_arrow()

# Sync API (internally uses asyncio.run())
db = lancedb.connect("./my-db")
table = db.open_table("tbl")  # Blocks until complete
results = table.search([0.1]*768).limit(10).to_arrow()
```

**Implementation**:
```python
class LanceTable(Table):
    """Synchronous wrapper around AsyncTable"""

    def search(self, query):
        # Delegates to async version using LOOP.run()
        return LOOP.run(self._async_table.search(query))
```

### 3. Fluent Query API

Chainable methods for query building:

```typescript
// TypeScript
const results = await table
  .search([0.1, 0.2, 0.3])
  .where("price < 100")
  .select(["id", "name", "price"])
  .limit(10)
  .toArrow();
```

```rust
// Rust
let results = table
    .query()
    .nearest_to(&[0.1, 0.2, 0.3])?
    .where_clause("price < 100")?
    .select(&["id", "name", "price"])?
    .limit(10)
    .execute()
    .await?;
```

---

## Embedding Integrations

### Supported Providers

| Provider | Features | Configuration |
|----------|----------|---------------|
| **OpenAI** | Text embeddings (Ada, v3) | API key |
| **AWS Bedrock** | Text embeddings (Titan, Cohere) | AWS credentials |
| **Sentence Transformers** | Local models (no API calls) | Model name |
| **Ollama** | Local LLMs via Ollama | Ollama endpoint |
| **Cohere** | Text + reranking | API key |
| **Jina** | Text embeddings | API key |
| **Gemini** | Multimodal embeddings | Google API key |

### Example: OpenAI Embeddings

```python
from lancedb.embeddings.openai import OpenAIEmbeddings

# Configure model
embedder = OpenAIEmbeddings(
    name="text-embedding-3-small",
    api_key="sk-...",
    dim=1536
)

# Auto-embed on insert
table = db.create_table(
    "documents",
    data=[{"text": "hello world", "id": 1}],
    embedding_functions=[embedder.create_source_field("text")]
)

# Auto-embed on search
results = table.search("greeting").limit(5).to_pandas()
```

### Custom Embeddings

Implement your own embedding model:

```python
from lancedb.embeddings import EmbeddingFunction

class MyEmbedder(EmbeddingFunction):
    def ndims(self):
        return 768

    def embed_documents(self, texts):
        # Your embedding logic here
        return [[0.1] * 768 for _ in texts]

    def embed_query(self, query):
        return [0.1] * 768

# Use custom embedder
embedder = MyEmbedder()
table = db.create_table("tbl", data=data, embedding_functions=[embedder])
```

---

## Remote vs Local Architecture

### Local Mode (NativeTable)

```
User Application
    │
    └─> LanceDB Library (in-process)
        │
        └─> Lance Format
            │
            └─> Object Store
                ├─> Local filesystem: ./my-db/
                ├─> S3: s3://bucket/my-db/
                ├─> GCS: gs://bucket/my-db/
                └─> Azure: az://container/my-db/
```

**Characteristics**:
- ✅ Zero latency (in-process)
- ✅ No network overhead
- ✅ Full feature set
- ❌ Single-machine scaling
- ❌ No multi-tenancy
- ❌ No authentication

### Remote Mode (RemoteTable)

```
User Application
    │
    └─> LanceDB Client Library
        │
        └─> HTTP/REST API
            │
            └─> LanceDB Cloud
                ├─> Authentication
                ├─> Multi-tenancy
                ├─> Managed scaling
                └─> Lance Format (S3/GCS backend)
```

**Characteristics**:
- ✅ Managed infrastructure
- ✅ Automatic scaling
- ✅ Multi-user access
- ✅ Authentication & authorization
- ❌ Network latency
- ❌ API rate limits

### Switching Between Modes

**Python**:
```python
# Local
db = lancedb.connect("./my-db")

# Remote (LanceDB Cloud)
db = lancedb.connect("db://my-instance", api_key="...")
```

**Rust**:
```rust
// Local
let db = lancedb::connect("./my-db").execute().await?;

// Remote (requires 'remote' feature)
let db = lancedb::connect("db://my-instance")
    .api_key("...")
    .execute()
    .await?;
```

### Feature Parity

| Feature | Local | Remote |
|---------|-------|--------|
| Vector search | ✅ | ✅ |
| Full-text search | ✅ | ✅ |
| SQL queries | ✅ | ✅ |
| Index creation | ✅ | ✅ |
| Versioning | ✅ | ✅ |
| Embeddings | ✅ | ✅ |
| Reranking | ✅ | ❌ (client-side) |

---

## Adding New Features

### Example: Adding a New Method on Table

Let's add a `compact()` method to reclaim space from deleted rows.

#### Step 1: Rust Core

```rust
// rust/lancedb/src/table.rs

#[async_trait]
pub trait BaseTable: Send + Sync {
    // ... existing methods

    /// Compact the table by merging fragments and applying deletes
    async fn compact(&mut self) -> Result<CompactionMetrics>;
}

impl BaseTable for NativeTable {
    async fn compact(&mut self) -> Result<CompactionMetrics> {
        // Call Lance dataset compact
        let metrics = self.dataset.compact_files(Default::default()).await?;

        Ok(CompactionMetrics {
            fragments_removed: metrics.fragments_removed,
            fragments_added: metrics.fragments_added,
            bytes_removed: metrics.bytes_removed,
        })
    }
}
```

```rust
// rust/lancedb/src/remote/table.rs

impl BaseTable for RemoteTable {
    async fn compact(&mut self) -> Result<CompactionMetrics> {
        // Call remote API
        let response = self.client
            .post(&format!("/v1/tables/{}/compact", self.name))
            .send()
            .await?;

        Ok(response.json().await?)
    }
}
```

```rust
// rust/lancedb/src/table.rs (public API)

impl Table {
    pub async fn compact(&mut self) -> Result<CompactionMetrics> {
        self.inner.compact().await
    }
}
```

#### Step 2: Python Bindings

```rust
// python/src/table.rs

#[pyclass]
pub struct PyTable {
    pub(crate) table: Arc<tokio::sync::Mutex<lancedb::Table>>,
}

#[pymethods]
impl PyTable {
    fn compact(&self, py: Python) -> PyResult<PyObject> {
        let table = self.table.clone();

        pyo3_asyncio::tokio::future_into_py(py, async move {
            let mut table = table.lock().await;
            let metrics = table.compact().await?;

            Python::with_gil(|py| {
                let dict = PyDict::new(py);
                dict.set_item("fragments_removed", metrics.fragments_removed)?;
                dict.set_item("fragments_added", metrics.fragments_added)?;
                dict.set_item("bytes_removed", metrics.bytes_removed)?;
                Ok(dict.to_object(py))
            })
        })
    }
}
```

```python
# python/python/lancedb/_lancedb.pyi

class Table:
    def compact(self) -> dict: ...
```

```python
# python/python/lancedb/table.py

class AsyncTable:
    async def compact(self) -> dict:
        """Compact the table by merging fragments.

        Returns
        -------
        dict
            Compaction metrics including fragments removed, added, and bytes reclaimed.
        """
        return await self._table.compact()

class LanceTable(Table):
    def compact(self) -> dict:
        """Compact the table (synchronous version)."""
        return LOOP.run(self._async_table.compact())
```

#### Step 3: TypeScript Bindings

```rust
// nodejs/src/table.rs

#[napi]
impl Table {
    #[napi]
    pub async fn compact(&mut self) -> napi::Result<Object> {
        let metrics = self.table.lock().await.compact().await?;

        // Convert to JS object
        let mut obj = Object::new();
        obj.set("fragmentsRemoved", metrics.fragments_removed)?;
        obj.set("fragmentsAdded", metrics.fragments_added)?;
        obj.set("bytesRemoved", metrics.bytes_removed)?;
        Ok(obj)
    }
}
```

```typescript
// nodejs/lancedb/native_table.ts

export class LocalTable extends Table {
  async compact(): Promise<CompactionMetrics> {
    return await this._tbl.compact();
  }
}

export interface CompactionMetrics {
  fragmentsRemoved: number;
  fragmentsAdded: number;
  bytesRemoved: number;
}
```

#### Step 4: Tests

```rust
// rust/lancedb/src/table.rs

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_compact() {
        let uri = "memory://test-compact";
        let db = crate::connect(uri).execute().await.unwrap();

        // Create table and add data
        let table = db.create_table("tbl", data).execute().await.unwrap();

        // Delete some rows
        table.delete("id < 50").await.unwrap();

        // Compact
        let metrics = table.compact().await.unwrap();
        assert!(metrics.fragments_removed > 0);
    }
}
```

```python
# python/tests/test_table.py

def test_compact(tmp_path):
    db = lancedb.connect(tmp_path)
    table = db.create_table("tbl", data=[{"id": i} for i in range(100)])

    # Delete half the rows
    table.delete("id < 50")

    # Compact
    metrics = table.compact()
    assert metrics["fragments_removed"] > 0
    assert metrics["bytes_removed"] > 0
```

```typescript
// nodejs/__test__/table.test.ts

test("compact table", async () => {
  const db = await lancedb.connect("memory://test");
  const table = await db.createTable("tbl", data);

  // Delete rows
  await table.delete("id < 50");

  // Compact
  const metrics = await table.compact();
  expect(metrics.fragmentsRemoved).toBeGreaterThan(0);
});
```

---

## Best Practices

### 1. Schema Design

```python
# ✅ Good: Use appropriate types
schema = {
    "id": int,
    "embedding": Vector(768),        # Use Vector type for embeddings
    "text": str,
    "metadata": dict,                # Flexible metadata
    "timestamp": datetime,
}

# ❌ Bad: Everything as strings
schema = {
    "id": str,
    "embedding": str,  # Don't store vectors as strings!
    "data": str,       # Don't JSON-encode everything
}
```

### 2. Index Strategy

```python
# ✅ Good: Index after loading data
table.add(large_dataset)
table.create_index("embedding", index_type="IVF_PQ")  # Index once

# ❌ Bad: Index before loading
table.create_index("embedding")  # Empty index!
table.add(large_dataset)         # Won't use index efficiently
```

### 3. Query Optimization

```python
# ✅ Good: Filter before vector search (prefilter)
results = (
    table
    .search([0.1]*768)
    .where("category = 'tech'", prefilter=True)  # Filter first
    .limit(10)
    .to_pandas()
)

# ❌ Bad: Filter after (postfilter) - wastes computation
results = (
    table
    .search([0.1]*768)
    .limit(10)
    .where("category = 'tech'")  # Filters already-retrieved results
    .to_pandas()
)
```

### 4. Batch Operations

```python
# ✅ Good: Batch inserts
table.add([
    {"text": "doc1", "vector": [0.1]*768},
    {"text": "doc2", "vector": [0.2]*768},
    # ... thousands of rows
])

# ❌ Bad: Row-by-row inserts
for doc in documents:
    table.add([doc])  # Creates new fragment each time!
```

### 5. Version Management

```python
# ✅ Good: Periodic cleanup
table.cleanup_old_versions(older_than=timedelta(days=7))

# ❌ Bad: Never cleanup
# Versions accumulate forever, wasting storage
```

---

## Resources

- **Documentation**: https://lancedb.com/docs
- **Python API**: https://lancedb.github.io/lancedb/python/python/
- **TypeScript API**: https://lancedb.github.io/lancedb/js/globals/
- **Rust API**: https://docs.rs/lancedb/latest/lancedb/
- **GitHub**: https://github.com/lancedb/lancedb
- **Discord**: https://discord.gg/lancedb
- **Blog**: https://blog.lancedb.com

---

**Welcome to LanceDB! 🚀**

Start with simple table creation and vector search, then explore advanced features like hybrid search, embeddings, and reranking. Check out the examples in each SDK directory for more inspiration.

Happy building!
