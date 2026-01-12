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

//! Builder for configuring a RADOS connection

use std::collections::HashMap;

use crate::{Error, Result};

use super::client::RadosClient;
use super::CephRados;

/// Configuration keys for RADOS
#[derive(Debug, Clone)]
pub enum RadosConfigKey {
    /// Ceph cluster name (default: "ceph")
    ClusterName,
    /// Ceph user name (default: "client.admin")
    UserName,
    /// Path to ceph.conf configuration file
    ConfFile,
    /// RADOS pool name (required)
    PoolName,
    /// Namespace within the pool (optional)
    Namespace,
    /// Path to keyring file for authentication
    Keyring,
    /// Direct authentication key (alternative to keyring)
    Key,
}

impl RadosConfigKey {
    /// Convert to string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ClusterName => "cluster_name",
            Self::UserName => "user_name",
            Self::ConfFile => "conf_file",
            Self::PoolName => "pool_name",
            Self::Namespace => "namespace",
            Self::Keyring => "keyring",
            Self::Key => "key",
        }
    }

    /// Parse from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "cluster_name" => Some(Self::ClusterName),
            "user_name" => Some(Self::UserName),
            "conf_file" => Some(Self::ConfFile),
            "pool_name" => Some(Self::PoolName),
            "namespace" => Some(Self::Namespace),
            "keyring" => Some(Self::Keyring),
            "key" => Some(Self::Key),
            _ => None,
        }
    }
}

/// Builder for RADOS object store
#[derive(Debug, Default)]
pub struct RadosBuilder {
    cluster_name: Option<String>,
    user_name: Option<String>,
    conf_file: Option<String>,
    pool_name: Option<String>,
    namespace: Option<String>,
    keyring: Option<String>,
    key: Option<String>,
    config: HashMap<String, String>,
}

impl RadosBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a builder from environment variables
    ///
    /// Reads the following environment variables:
    /// - `CEPH_CLUSTER_NAME` or `RADOS_CLUSTER_NAME`
    /// - `CEPH_USER_NAME` or `RADOS_USER_NAME`
    /// - `CEPH_CONF` or `RADOS_CONF`
    /// - `CEPH_POOL` or `RADOS_POOL`
    /// - `CEPH_NAMESPACE` or `RADOS_NAMESPACE`
    /// - `CEPH_KEYRING` or `RADOS_KEYRING`
    /// - `CEPH_KEY` or `RADOS_KEY`
    pub fn from_env() -> Self {
        let mut builder = Self::new();

        if let Ok(v) = std::env::var("CEPH_CLUSTER_NAME").or_else(|_| std::env::var("RADOS_CLUSTER_NAME")) {
            builder = builder.with_cluster_name(v);
        }

        if let Ok(v) = std::env::var("CEPH_USER_NAME").or_else(|_| std::env::var("RADOS_USER_NAME")) {
            builder = builder.with_user_name(v);
        }

        if let Ok(v) = std::env::var("CEPH_CONF").or_else(|_| std::env::var("RADOS_CONF")) {
            builder = builder.with_conf_file(v);
        }

        if let Ok(v) = std::env::var("CEPH_POOL").or_else(|_| std::env::var("RADOS_POOL")) {
            builder = builder.with_pool_name(v);
        }

        if let Ok(v) = std::env::var("CEPH_NAMESPACE").or_else(|_| std::env::var("RADOS_NAMESPACE")) {
            builder = builder.with_namespace(v);
        }

        if let Ok(v) = std::env::var("CEPH_KEYRING").or_else(|_| std::env::var("RADOS_KEYRING")) {
            builder = builder.with_keyring(v);
        }

        if let Ok(v) = std::env::var("CEPH_KEY").or_else(|_| std::env::var("RADOS_KEY")) {
            builder = builder.with_key(v);
        }

        builder
    }

    /// Set the cluster name
    pub fn with_cluster_name(mut self, cluster_name: impl Into<String>) -> Self {
        self.cluster_name = Some(cluster_name.into());
        self
    }

    /// Set the user name
    pub fn with_user_name(mut self, user_name: impl Into<String>) -> Self {
        self.user_name = Some(user_name.into());
        self
    }

    /// Set the configuration file path
    pub fn with_conf_file(mut self, conf_file: impl Into<String>) -> Self {
        self.conf_file = Some(conf_file.into());
        self
    }

    /// Set the pool name (required)
    pub fn with_pool_name(mut self, pool_name: impl Into<String>) -> Self {
        self.pool_name = Some(pool_name.into());
        self
    }

    /// Set the namespace
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Set the keyring file path
    pub fn with_keyring(mut self, keyring: impl Into<String>) -> Self {
        self.keyring = Some(keyring.into());
        self
    }

    /// Set the authentication key directly
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set a configuration option by key
    pub fn with_config(mut self, key: RadosConfigKey, value: impl Into<String>) -> Self {
        self.config.insert(key.as_str().to_string(), value.into());
        self
    }

    /// Parse configuration from a map
    pub fn with_config_map(mut self, config: HashMap<String, String>) -> Self {
        for (key, value) in config {
            if let Some(config_key) = RadosConfigKey::from_str(&key) {
                match config_key {
                    RadosConfigKey::ClusterName => self.cluster_name = Some(value),
                    RadosConfigKey::UserName => self.user_name = Some(value),
                    RadosConfigKey::ConfFile => self.conf_file = Some(value),
                    RadosConfigKey::PoolName => self.pool_name = Some(value),
                    RadosConfigKey::Namespace => self.namespace = Some(value),
                    RadosConfigKey::Keyring => self.keyring = Some(value),
                    RadosConfigKey::Key => self.key = Some(value),
                }
            }
        }
        self
    }

    /// Build the RADOS object store
    pub fn build(self) -> Result<CephRados> {
        // Pool name is required
        let pool_name = self.pool_name.ok_or_else(|| {
            Error::Generic {
                store: "RADOS",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "pool_name is required",
                )),
            }
        })?;

        // Use defaults for optional fields
        let cluster_name = self.cluster_name.unwrap_or_else(|| "ceph".to_string());
        let user_name = self.user_name.unwrap_or_else(|| "admin".to_string());

        let client = RadosClient::new(
            cluster_name,
            user_name,
            self.conf_file,
            pool_name,
            self.namespace,
            self.keyring,
            self.key,
        )?;

        Ok(CephRados::new(client))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_key_parsing() {
        assert!(matches!(
            RadosConfigKey::from_str("pool_name"),
            Some(RadosConfigKey::PoolName)
        ));
        assert!(matches!(
            RadosConfigKey::from_str("cluster_name"),
            Some(RadosConfigKey::ClusterName)
        ));
        assert!(RadosConfigKey::from_str("invalid").is_none());
    }

    #[test]
    fn test_builder_defaults() {
        let builder = RadosBuilder::new()
            .with_pool_name("test-pool");

        // Should use defaults for cluster and user
        assert!(builder.pool_name.is_some());
    }

    #[test]
    fn test_builder_with_config() {
        let builder = RadosBuilder::new()
            .with_cluster_name("my-cluster")
            .with_user_name("client.test")
            .with_pool_name("my-pool")
            .with_namespace("my-ns")
            .with_keyring("/etc/ceph/keyring");

        assert_eq!(builder.cluster_name.as_deref(), Some("my-cluster"));
        assert_eq!(builder.user_name.as_deref(), Some("client.test"));
        assert_eq!(builder.pool_name.as_deref(), Some("my-pool"));
        assert_eq!(builder.namespace.as_deref(), Some("my-ns"));
        assert_eq!(builder.keyring.as_deref(), Some("/etc/ceph/keyring"));
    }

    #[test]
    fn test_config_map() {
        let mut config = HashMap::new();
        config.insert("pool_name".to_string(), "test-pool".to_string());
        config.insert("cluster_name".to_string(), "test-cluster".to_string());

        let builder = RadosBuilder::new().with_config_map(config);

        assert_eq!(builder.pool_name.as_deref(), Some("test-pool"));
        assert_eq!(builder.cluster_name.as_deref(), Some("test-cluster"));
    }
}
