# Ceph RADOS Credential Configuration Guide

This document explains how to configure credentials for the Ceph RADOS backend in arrow-rs-object-store.

## Overview

The RADOS backend supports multiple ways to provide authentication credentials, following Ceph's standard authentication mechanisms. Credentials can be specified via:

1. **Keyring files** (most common for production)
2. **Direct authentication key** (useful for programmatic access)
3. **Configuration file** (ceph.conf can contain auth settings)
4. **Environment variables** (convenient for containerized environments)
5. **Default keyring locations** (automatic discovery)

## Authentication Methods

### Method 1: Keyring File (Recommended for Production)

Specify the path to a keyring file containing the authentication key:

```rust
use object_store::rados::RadosBuilder;

let rados = RadosBuilder::new()
    .with_pool_name("my-pool")
    .with_user_name("client.myapp")
    .with_keyring("/etc/ceph/ceph.client.myapp.keyring")
    .build()?;
```

**Keyring file format:**
```ini
[client.myapp]
    key = AQCvCbtToC6MDhAATtuT70Sl+DymPCfDSsyV4w==
```

### Method 2: Direct Key (Recommended for Secrets Management)

Provide the authentication key directly (base64 encoded):

```rust
use object_store::rados::RadosBuilder;

let rados = RadosBuilder::new()
    .with_pool_name("my-pool")
    .with_user_name("client.myapp")
    .with_key("AQCvCbtToC6MDhAATtuT70Sl+DymPCfDSsyV4w==")
    .build()?;
```

This is particularly useful when integrating with secrets management systems like:
- HashiCorp Vault
- Kubernetes Secrets
- AWS Secrets Manager
- Environment variables

### Method 3: Configuration File

Include authentication settings in ceph.conf:

```ini
[client.myapp]
    keyring = /path/to/keyring
```

Then specify only the config file:

```rust
let rados = RadosBuilder::new()
    .with_pool_name("my-pool")
    .with_user_name("client.myapp")
    .with_conf_file("/etc/ceph/ceph.conf")
    .build()?;
```

### Method 4: Environment Variables

Configure via environment variables:

```bash
export CEPH_POOL=my-pool
export CEPH_USER_NAME=client.myapp
export CEPH_KEYRING=/etc/ceph/ceph.client.myapp.keyring
# OR
export CEPH_KEY=AQCvCbtToC6MDhAATtuT70Sl+DymPCfDSsyV4w==
```

```rust
let rados = RadosBuilder::from_env().build()?;
```

**Supported environment variables:**
- `CEPH_CLUSTER_NAME` - Cluster name (default: "ceph")
- `CEPH_USER_NAME` - User name (default: "client.admin")
- `CEPH_CONF` - Path to ceph.conf
- `CEPH_POOL` - Pool name (required)
- `CEPH_NAMESPACE` - Optional namespace
- `CEPH_KEYRING` - Path to keyring file
- `CEPH_KEY` - Authentication key (base64 encoded)

### Method 5: Default Keyring Locations

If no credentials are explicitly provided, librados will automatically search for keyrings in:

1. `/etc/ceph/ceph.client.<user>.keyring`
2. `/etc/ceph/ceph.keyring`
3. `/etc/ceph/keyring`
4. `~/.ceph/keyring`

```rust
let rados = RadosBuilder::new()
    .with_pool_name("my-pool")
    .with_user_name("client.admin")  // Will look for /etc/ceph/ceph.client.admin.keyring
    .build()?;
```

## Complete Examples

### Example 1: Production Deployment with Keyring File

```rust
use object_store::rados::RadosBuilder;
use object_store::ObjectStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rados = RadosBuilder::new()
        .with_cluster_name("production")
        .with_user_name("client.myapp")
        .with_pool_name("data-pool")
        .with_keyring("/etc/ceph/ceph.client.myapp.keyring")
        .with_conf_file("/etc/ceph/ceph.conf")
        .build()?;

    // Use the object store
    let path = object_store::path::Path::from("test.txt");
    rados.put(&path, "Hello, RADOS!".into()).await?;

    Ok(())
}
```

### Example 2: Kubernetes with Secrets

Deploy keyring as a Kubernetes secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: ceph-credentials
type: Opaque
data:
  key: QVFDdkNidFRvQzZNRGhBQVR0dVQ3MFNsK0R5bVBDZkRTc3lWNHc9PQ==  # base64 encoded key
```

Mount and use in application:

```rust
use std::fs;

let key = fs::read_to_string("/etc/ceph-secret/key")?;
let rados = RadosBuilder::new()
    .with_pool_name(std::env::var("CEPH_POOL")?)
    .with_user_name("client.k8s-app")
    .with_key(key.trim())
    .build()?;
```

### Example 3: Docker Compose with Environment Variables

```yaml
version: '3.8'
services:
  app:
    image: myapp:latest
    environment:
      CEPH_CLUSTER_NAME: docker-ceph
      CEPH_USER_NAME: client.docker
      CEPH_POOL: docker-pool
      CEPH_KEY: ${CEPH_AUTH_KEY}  # Set in .env file
    volumes:
      - ./ceph.conf:/etc/ceph/ceph.conf:ro
```

```rust
// Application code
let rados = RadosBuilder::from_env().build()?;
```

### Example 4: Configuration via Key-Value Pairs

```rust
use object_store::rados::{RadosBuilder, RadosConfigKey};

let config = vec![
    (RadosConfigKey::ClusterName, "production"),
    (RadosConfigKey::UserName, "client.myapp"),
    (RadosConfigKey::PoolName, "data-pool"),
    (RadosConfigKey::Keyring, "/etc/ceph/ceph.client.myapp.keyring"),
];

let mut builder = RadosBuilder::new();
for (key, value) in config {
    builder = builder.with_config(key, value);
}
let rados = builder.build()?;
```

## Credential Hierarchy

When multiple credential sources are specified, the following priority order applies:

1. **Direct key** (`with_key()`) - highest priority
2. **Keyring file** (`with_keyring()`)
3. **Configuration file settings** (`with_conf_file()`)
4. **Environment variables**
5. **Default keyring locations** - lowest priority

## Security Best Practices

### ✅ DO:

1. **Use keyring files in production** - Store at `/etc/ceph/` with proper permissions (0600)
2. **Rotate credentials regularly** - Update keyring files periodically
3. **Use namespaces** - Separate different applications within the same pool
4. **Restrict permissions** - Ensure keyring files are readable only by the application user
5. **Use secrets management** - For cloud deployments, use native secrets services
6. **Set minimal capabilities** - Grant only necessary RADOS permissions to each user

### ❌ DON'T:

1. **Hard-code credentials** - Never put keys directly in source code
2. **Commit keyring files** - Add `*.keyring` to `.gitignore`
3. **Use admin credentials** - Create dedicated users with minimal permissions
4. **Share credentials** - Each application should have its own RADOS user
5. **Log credentials** - Be careful with debug output
6. **Use world-readable files** - Always set restrictive permissions on keyrings

## Getting Credentials from Ceph

### Create a new Ceph user for your application:

```bash
# Create user with access to specific pool
ceph auth get-or-create client.myapp \
  mon 'profile rbd' \
  osd 'profile rbd pool=my-pool' \
  -o /etc/ceph/ceph.client.myapp.keyring

# View the generated key
ceph auth get client.myapp
```

### Extract just the key value:

```bash
# Get base64-encoded key
ceph auth print-key client.myapp
```

### Set appropriate permissions:

```bash
chown myapp:myapp /etc/ceph/ceph.client.myapp.keyring
chmod 0600 /etc/ceph/ceph.client.myapp.keyring
```

## Troubleshooting

### Permission Denied

**Error:** `rados_connect failed: Permission denied`

**Solutions:**
1. Verify the key is correct: `ceph auth get client.myapp`
2. Check keyring file permissions: `ls -l /etc/ceph/ceph.client.myapp.keyring`
3. Ensure the user has appropriate capabilities for the pool
4. Verify cluster name matches

### Keyring Not Found

**Error:** `unable to find a keyring`

**Solutions:**
1. Specify keyring path explicitly with `with_keyring()`
2. Check default locations: `/etc/ceph/ceph.client.<user>.keyring`
3. Use `with_key()` to provide key directly
4. Verify `CEPH_KEYRING` environment variable

### Wrong User

**Error:** `RADOS client with user name 'admin' not found`

**Solutions:**
1. Ensure user exists: `ceph auth list | grep client.myapp`
2. Check user_name matches keyring file name
3. Verify `with_user_name()` matches the created user

## Librados Implementation Notes

When integrating with actual librados, credentials are configured using:

```c
// C API example for reference
rados_t cluster;
rados_create(&cluster, "client.myapp");
rados_conf_read_file(cluster, "/etc/ceph/ceph.conf");

// Method 1: Set keyring path
rados_conf_set(cluster, "keyring", "/path/to/keyring");

// Method 2: Set key directly
rados_conf_set(cluster, "key", "AQCvCbtToC6MDhAATtuT70Sl+DymPCfDSsyV4w==");

rados_connect(cluster);
```

The Rust `ceph` crate provides equivalent functionality through its API.

## References

- [Ceph Authentication Documentation](https://docs.ceph.com/en/latest/rados/operations/user-management/)
- [Ceph User Management](https://docs.ceph.com/en/latest/rados/operations/user-management/)
- [Cephx Authentication](https://docs.ceph.com/en/latest/rados/configuration/auth-config-ref/)
