# Ceph RADOS Backend Implementation for arrow-rs-object-store

This document describes the Ceph RADOS object store backend implementation that has been added to the arrow-rs-object-store project.

## Overview

A new backend for Ceph RADOS has been implemented, providing an ObjectStore interface for interacting with Ceph object storage through the librados API.

## Files Created

### Module Structure

```
arrow-rs-object-store/src/rados/
├── mod.rs           # Main module with ObjectStore implementation
├── builder.rs       # RadosBuilder for configuration
├── client.rs        # RadosClient for RADOS API interactions
└── tests.rs         # Unit tests
```

### Core Components

1. **RadosBuilder** (`builder.rs`)
   - Configuration builder pattern for Ceph RADOS connections
   - Supports configuration via builder methods or environment variables
   - Configuration keys:
     - `cluster_name`: Ceph cluster name (default: "ceph")
     - `user_name`: Ceph user (default: "client.admin")
     - `conf_file`: Path to ceph.conf file
     - `pool_name`: RADOS pool name (required)
     - `namespace`: Optional namespace within pool

2. **RadosClient** (`client.rs`)
   - Core client for RADOS operations
   - Implements all ObjectStore operations
   - Currently contains placeholder implementations with TODO comments for actual librados integration

3. **CephRados** (`mod.rs`)
   - Main ObjectStore implementation
   - Wraps RadosClient and delegates operations

## Configuration

### Example Usage

```rust
use object_store::rados::RadosBuilder;

// Build from explicit configuration
let rados = RadosBuilder::new()
    .with_pool_name("my-pool")
    .with_cluster_name("ceph")
    .with_user_name("client.admin")
    .with_conf_file("/etc/ceph/ceph.conf")
    .build()?;

// Or from environment variables
let rados = RadosBuilder::from_env()
    .with_pool_name("my-pool")
    .build()?;
```

### Environment Variables

- `CEPH_CLUSTER_NAME`: Cluster name
- `CEPH_USER_NAME`: User name
- `CEPH_CONF`: Configuration file path
- `CEPH_POOL`: Pool name
- `CEPH_NAMESPACE`: Namespace

## Integration Points

The implementation is integrated into the main object_store crate:

1. **Cargo.toml**: Added `rados` feature flag and `ceph` dependency
2. **lib.rs**: Module declaration and documentation

## Current Status

### ✅ Completed

- Module structure and builder pattern
- Configuration handling
- ObjectStore trait skeleton
- Feature flag integration
- Basic unit tests
- Documentation

### ⚠️ TODO: Actual librados Integration

The current implementation contains placeholder code. To complete the integration with actual Ceph RADOS, you need to:

#### 1. Update RadosClient to use librados

Replace placeholder code in `client.rs` with actual `ceph` crate calls. For each operation:

**put_opts**:
```rust
// TODO: Integrate with actual librados bindings
// 1. Create cluster handle: rados_create(&cluster, user_name)
// 2. Read config: rados_conf_read_file(cluster, conf_file)
// 3. Connect: rados_connect(cluster)
// 4. Create IO context: rados_ioctx_create(cluster, pool_name, &io)
// 5. Set namespace if needed: rados_ioctx_set_namespace(io, namespace)
// 6. Write object: rados_write_full(io, object_name, data, len)
// 7. Cleanup: rados_ioctx_destroy(io); rados_shutdown(cluster)
```

**get_opts**:
```rust
// TODO: Handle range reads with rados_read(io, object, buffer, len, offset)
// Return object metadata and data
```

**delete_stream**:
```rust
// TODO: Use rados_remove(io, object_name) for each object
```

**list**:
```rust
// TODO: Use rados_nobjects_list_open/next to iterate objects
// Filter by prefix and yield ObjectMeta
```

**copy_opts**:
```rust
// TODO: RADOS doesn't have native copy, implement as read + write
// Handle CopyOptions (overwrite vs create)
```

#### 2. Connection Pooling

Consider implementing connection pooling to avoid creating a new RADOS connection for each operation:

```rust
pub struct RadosClient {
    connection_pool: Arc<RadosConnectionPool>,
    pool_name: String,
    namespace: Option<String>,
}
```

#### 3. Error Handling

Map RADOS errors to object_store Error types:

```rust
fn map_rados_error(e: rados::Error) -> crate::Error {
    match e {
        rados::Error::NotFound => Error::NotFound { ... },
        rados::Error::AlreadyExists => Error::AlreadyExists { ... },
        // ... other mappings
    }
}
```

#### 4. Multipart Upload

Implement multipart upload support. Since RADOS doesn't have native multipart:

1. Create temporary objects for each part with naming convention
2. On complete, concatenate parts into final object using rados append operations
3. On abort, delete temporary parts

#### 5. Async Operations

Consider using RADOS AIO (async I/O) operations for better performance:

```rust
// Use rados_aio_create_completion() and rados_aio_write() instead of blocking calls
```

## Testing

### Unit Tests

Basic unit tests are in `src/rados/tests.rs`. They test:
- Builder configuration
- Config key parsing
- Environment variable handling

### Integration Tests

To run integration tests against a real Ceph cluster:

```bash
# Set up test environment
export CEPH_POOL=test-pool
export CEPH_CONF=/etc/ceph/ceph.conf

# Run tests with rados feature
cargo test --features rados
```

## Building

To build with RADOS support:

```bash
cargo build --features rados
```

To use in your project:

```toml
[dependencies]
object_store = { version = "0.13", features = ["rados"] }
```

## Dependencies

- `ceph` (version 3.1.0): Rust bindings for librados
- Requires librados development libraries to be installed on the system

## References

- [Ceph RADOS API Documentation](https://docs.ceph.com/en/latest/rados/api/)
- [librados Documentation](https://docs.ceph.com/en/latest/rados/api/librados/)
- [Rust ceph crate](https://docs.rs/ceph)
- [Rust ceph crate](https://crates.io/crates/ceph)
- [rad crate (higher-level RADOS bindings)](https://crates.io/crates/rad)

## Next Steps

1. Choose between `ceph` crate (low-level FFI) or `rad` crate (high-level) for implementation
2. Implement actual RADOS operations in RadosClient
3. Add proper error mapping
4. Implement connection pooling for efficiency
5. Add comprehensive integration tests
6. Consider async I/O operations for performance
7. Implement multipart upload support
8. Add metrics and logging
9. Performance benchmarking against other backends

## Notes

- The `rados` feature flag ensures the RADOS backend is optional
- The implementation follows the same patterns as existing backends (AWS, Azure, GCP)
- All public APIs are documented
- The code is properly licensed (Apache 2.0)
