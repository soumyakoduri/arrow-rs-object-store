// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::rados::client::RadosClient;
use crate::rados::{CephRados, STORE};
use crate::Result;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;

/// A specialized `Error` for RADOS-related errors
#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Missing pool name")]
    MissingPoolName,

    #[error("Configuration key: '{}' is not known.", key)]
    UnknownConfigurationKey { key: String },
}

impl From<Error> for crate::Error {
    fn from(source: Error) -> Self {
        match source {
            Error::UnknownConfigurationKey { key } => {
                Self::UnknownConfigurationKey { store: STORE, key }
            }
            _ => Self::Generic {
                store: STORE,
                source: Box::new(source),
            },
        }
    }
}

/// Configure a connection to Ceph RADOS using the specified credentials and pool.
///
/// # Example
/// ```no_run
/// # use object_store::rados::RadosBuilder;
/// # async fn example() -> object_store::Result<()> {
/// let rados = RadosBuilder::new()
///     .with_pool_name("my-pool")
///     .with_cluster_name("ceph")
///     .with_user_name("client.admin")
///     .with_conf_file("/etc/ceph/ceph.conf")
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Default, Clone)]
pub struct RadosBuilder {
    /// Ceph cluster name (default: "ceph")
    cluster_name: Option<String>,
    /// Ceph user name (default: "client.admin")
    user_name: Option<String>,
    /// Path to Ceph configuration file
    conf_file: Option<String>,
    /// Pool name to use for object storage
    pool_name: Option<String>,
    /// Optional namespace within the pool
    namespace: Option<String>,
    /// Path to keyring file for authentication
    keyring: Option<String>,
    /// Authentication key (alternative to keyring file)
    key: Option<String>,
}

/// Configuration keys for [`RadosBuilder`]
///
/// Configuration via keys can be done via [`RadosBuilder::with_config`]
///
/// # Example
/// ```no_run
/// # use object_store::rados::{RadosBuilder, RadosConfigKey};
/// let builder = RadosBuilder::new()
///     .with_config("pool_name".parse().unwrap(), "my-pool")
///     .with_config(RadosConfigKey::ClusterName, "ceph");
/// ```
#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy, Serialize, Deserialize)]
#[non_exhaustive]
pub enum RadosConfigKey {
    /// Ceph cluster name
    ///
    /// See [`RadosBuilder::with_cluster_name`] for details.
    ///
    /// Supported keys:
    /// - `cluster_name`
    /// - `ceph_cluster_name`
    ClusterName,

    /// Ceph user name
    ///
    /// See [`RadosBuilder::with_user_name`] for details.
    ///
    /// Supported keys:
    /// - `user_name`
    /// - `ceph_user_name`
    /// - `ceph_user`
    UserName,

    /// Path to Ceph configuration file
    ///
    /// See [`RadosBuilder::with_conf_file`] for details.
    ///
    /// Supported keys:
    /// - `conf_file`
    /// - `ceph_conf`
    /// - `ceph_conf_file`
    ConfFile,

    /// Pool name
    ///
    /// See [`RadosBuilder::with_pool_name`] for details.
    ///
    /// Supported keys:
    /// - `pool`
    /// - `pool_name`
    /// - `ceph_pool`
    PoolName,

    /// Namespace within the pool
    ///
    /// See [`RadosBuilder::with_namespace`] for details.
    ///
    /// Supported keys:
    /// - `namespace`
    /// - `ceph_namespace`
    Namespace,

    /// Path to keyring file
    ///
    /// See [`RadosBuilder::with_keyring`] for details.
    ///
    /// Supported keys:
    /// - `keyring`
    /// - `ceph_keyring`
    Keyring,

    /// Authentication key
    ///
    /// See [`RadosBuilder::with_key`] for details.
    ///
    /// Supported keys:
    /// - `key`
    /// - `ceph_key`
    Key,

    /// Client configuration
    Client(crate::ClientConfigKey),
}

impl AsRef<str> for RadosConfigKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::ClusterName => "cluster_name",
            Self::UserName => "user_name",
            Self::ConfFile => "conf_file",
            Self::PoolName => "pool_name",
            Self::Namespace => "namespace",
            Self::Keyring => "keyring",
            Self::Key => "key",
            Self::Client(key) => key.as_ref(),
        }
    }
}

impl FromStr for RadosConfigKey {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "cluster_name" | "ceph_cluster_name" => Ok(Self::ClusterName),
            "user_name" | "ceph_user_name" | "ceph_user" => Ok(Self::UserName),
            "conf_file" | "ceph_conf" | "ceph_conf_file" => Ok(Self::ConfFile),
            "pool" | "pool_name" | "ceph_pool" => Ok(Self::PoolName),
            "namespace" | "ceph_namespace" => Ok(Self::Namespace),
            "keyring" | "ceph_keyring" => Ok(Self::Keyring),
            "key" | "ceph_key" => Ok(Self::Key),
            _ => match s.parse() {
                Ok(key) => Ok(Self::Client(key)),
                Err(_) => Err(Error::UnknownConfigurationKey {
                    key: s.to_string(),
                }),
            },
        }
    }
}

impl RadosBuilder {
    /// Create a new [`RadosBuilder`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the Ceph cluster name (default: "ceph")
    pub fn with_cluster_name(mut self, cluster_name: impl Into<String>) -> Self {
        self.cluster_name = Some(cluster_name.into());
        self
    }

    /// Set the Ceph user name (default: "client.admin")
    pub fn with_user_name(mut self, user_name: impl Into<String>) -> Self {
        self.user_name = Some(user_name.into());
        self
    }

    /// Set the path to the Ceph configuration file
    ///
    /// If not specified, librados will use the default locations:
    /// - $CEPH_CONF (environment variable)
    /// - /etc/ceph/ceph.conf
    /// - ~/.ceph/config
    /// - ./ceph.conf
    pub fn with_conf_file(mut self, conf_file: impl Into<String>) -> Self {
        self.conf_file = Some(conf_file.into());
        self
    }

    /// Set the pool name to use for object storage (required)
    pub fn with_pool_name(mut self, pool_name: impl Into<String>) -> Self {
        self.pool_name = Some(pool_name.into());
        self
    }

    /// Set the namespace within the pool
    ///
    /// Namespaces provide logical separation within a pool
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Set the path to the keyring file for authentication
    ///
    /// If not specified, librados will use default keyring locations:
    /// - /etc/ceph/ceph.client.<user>.keyring
    /// - /etc/ceph/keyring
    /// - ~/.ceph/keyring
    ///
    /// # Example
    /// ```no_run
    /// # use object_store::rados::RadosBuilder;
    /// let rados = RadosBuilder::new()
    ///     .with_pool_name("my-pool")
    ///     .with_user_name("client.myapp")
    ///     .with_keyring("/etc/ceph/ceph.client.myapp.keyring")
    ///     .build();
    /// ```
    pub fn with_keyring(mut self, keyring: impl Into<String>) -> Self {
        self.keyring = Some(keyring.into());
        self
    }

    /// Set the authentication key directly (base64 encoded)
    ///
    /// This is an alternative to using a keyring file. Useful for
    /// programmatic access or when credentials are stored in environment
    /// variables or secrets management systems.
    ///
    /// # Example
    /// ```no_run
    /// # use object_store::rados::RadosBuilder;
    /// let rados = RadosBuilder::new()
    ///     .with_pool_name("my-pool")
    ///     .with_user_name("client.myapp")
    ///     .with_key("AQCvCbtToC6MDhAATtuT70Sl+DymPCfDSsyV4w==")
    ///     .build();
    /// ```
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set a configuration option using a key-value pair
    pub fn with_config(mut self, key: RadosConfigKey, value: impl Into<String>) -> Self {
        match key {
            RadosConfigKey::ClusterName => self.cluster_name = Some(value.into()),
            RadosConfigKey::UserName => self.user_name = Some(value.into()),
            RadosConfigKey::ConfFile => self.conf_file = Some(value.into()),
            RadosConfigKey::PoolName => self.pool_name = Some(value.into()),
            RadosConfigKey::Namespace => self.namespace = Some(value.into()),
            RadosConfigKey::Keyring => self.keyring = Some(value.into()),
            RadosConfigKey::Key => self.key = Some(value.into()),
            RadosConfigKey::Client(_key) => {
                // Client configuration would be handled here
                // For now, we'll skip it as we don't have ClientOptions yet
            }
        }
        self
    }

    /// Build the [`CephRados`] instance
    pub fn build(self) -> Result<CephRados> {
        let pool_name = self.pool_name.ok_or(Error::MissingPoolName)?;

        let client = RadosClient::new(
            self.cluster_name.unwrap_or_else(|| "ceph".to_string()),
            self.user_name.unwrap_or_else(|| "client.admin".to_string()),
            self.conf_file,
            pool_name,
            self.namespace,
            self.keyring,
            self.key,
        )?;

        Ok(CephRados::new(Arc::new(client)))
    }

    /// Create a [`RadosBuilder`] from environment variables
    ///
    /// The following environment variables are supported:
    /// - `CEPH_CLUSTER_NAME`: Cluster name
    /// - `CEPH_USER_NAME`: User name
    /// - `CEPH_CONF`: Configuration file path
    /// - `CEPH_POOL`: Pool name
    /// - `CEPH_NAMESPACE`: Namespace
    /// - `CEPH_KEYRING`: Path to keyring file
    /// - `CEPH_KEY`: Authentication key (base64 encoded)
    pub fn from_env() -> Self {
        let mut builder = Self::new();

        if let Ok(cluster_name) = std::env::var("CEPH_CLUSTER_NAME") {
            builder = builder.with_cluster_name(cluster_name);
        }

        if let Ok(user_name) = std::env::var("CEPH_USER_NAME") {
            builder = builder.with_user_name(user_name);
        }

        if let Ok(conf_file) = std::env::var("CEPH_CONF") {
            builder = builder.with_conf_file(conf_file);
        }

        if let Ok(pool_name) = std::env::var("CEPH_POOL") {
            builder = builder.with_pool_name(pool_name);
        }

        if let Ok(namespace) = std::env::var("CEPH_NAMESPACE") {
            builder = builder.with_namespace(namespace);
        }

        if let Ok(keyring) = std::env::var("CEPH_KEYRING") {
            builder = builder.with_keyring(keyring);
        }

        if let Ok(key) = std::env::var("CEPH_KEY") {
            builder = builder.with_key(key);
        }

        builder
    }
}
