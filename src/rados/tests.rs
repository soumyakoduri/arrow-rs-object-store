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

//! Tests for RADOS backend
//!
//! Note: Integration tests require a running Ceph cluster.
//! Unit tests can run without a cluster.

#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn test_builder_creation() {
        let builder = RadosBuilder::new()
            .with_pool_name("test-pool")
            .with_cluster_name("ceph")
            .with_user_name("admin");

        // Builder should accept all configuration
        assert!(builder.pool_name.is_some());
        assert!(builder.cluster_name.is_some());
    }

    #[test]
    fn test_config_key_as_str() {
        assert_eq!(RadosConfigKey::PoolName.as_str(), "pool_name");
        assert_eq!(RadosConfigKey::ClusterName.as_str(), "cluster_name");
        assert_eq!(RadosConfigKey::UserName.as_str(), "user_name");
        assert_eq!(RadosConfigKey::ConfFile.as_str(), "conf_file");
        assert_eq!(RadosConfigKey::Namespace.as_str(), "namespace");
        assert_eq!(RadosConfigKey::Keyring.as_str(), "keyring");
        assert_eq!(RadosConfigKey::Key.as_str(), "key");
    }

    #[test]
    fn test_config_key_from_str() {
        assert!(matches!(
            RadosConfigKey::from_str("pool_name"),
            Some(RadosConfigKey::PoolName)
        ));
        assert!(matches!(
            RadosConfigKey::from_str("cluster_name"),
            Some(RadosConfigKey::ClusterName)
        ));
        assert!(RadosConfigKey::from_str("invalid_key").is_none());
    }

    #[test]
    fn test_builder_requires_pool() {
        let builder = RadosBuilder::new()
            .with_cluster_name("ceph")
            .with_user_name("admin");

        // Should fail without pool_name
        assert!(builder.build().is_err());
    }

    #[test]
    fn test_builder_with_all_options() {
        let builder = RadosBuilder::new()
            .with_pool_name("test-pool")
            .with_cluster_name("my-cluster")
            .with_user_name("client.test")
            .with_conf_file("/etc/ceph/ceph.conf")
            .with_namespace("my-namespace")
            .with_keyring("/etc/ceph/keyring")
            .with_key("AQD1234567890==");

        assert_eq!(builder.pool_name.as_deref(), Some("test-pool"));
        assert_eq!(builder.cluster_name.as_deref(), Some("my-cluster"));
        assert_eq!(builder.user_name.as_deref(), Some("client.test"));
        assert_eq!(builder.conf_file.as_deref(), Some("/etc/ceph/ceph.conf"));
        assert_eq!(builder.namespace.as_deref(), Some("my-namespace"));
        assert_eq!(builder.keyring.as_deref(), Some("/etc/ceph/keyring"));
        assert_eq!(builder.key.as_deref(), Some("AQD1234567890=="));
    }

    #[test]
    fn test_env_var_parsing() {
        // This test would need to set environment variables
        // For now, just verify the method exists
        let _builder = RadosBuilder::from_env();
    }

    // Integration tests that require a running Ceph cluster
    // These are disabled by default and can be enabled with:
    // cargo test --features rados -- --ignored

    #[test]
    #[ignore]
    fn test_connection_to_cluster() {
        // This test requires:
        // - A running Ceph cluster
        // - Environment variables set:
        //   - CEPH_POOL=test-pool
        //   - CEPH_CONF=/etc/ceph/ceph.conf
        //   - CEPH_KEYRING=/etc/ceph/ceph.client.admin.keyring

        let builder = RadosBuilder::from_env();

        // Attempt to build (will fail if cluster not available)
        let result = builder.build();

        // If this succeeds, we have a valid connection
        if let Ok(store) = result {
            println!("Successfully connected to RADOS: {}", store);
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_put_get_object() {
        use crate::{ObjectStore, Path};

        // Requires running Ceph cluster
        let store = RadosBuilder::from_env()
            .build()
            .expect("Failed to create RADOS store");

        let path = Path::from("test/object.txt");
        let data = b"Hello, RADOS!";

        // Put object
        store
            .put(&path, data.to_vec().into())
            .await
            .expect("Failed to put object");

        // Get object
        let result = store.get(&path).await.expect("Failed to get object");
        let bytes = result.bytes().await.expect("Failed to read bytes");

        assert_eq!(&bytes[..], data);

        // Clean up
        store.delete(&path).await.expect("Failed to delete object");
    }

    #[tokio::test]
    #[ignore]
    async fn test_list_objects() {
        use crate::{ObjectStore, Path};
        use futures::StreamExt;

        // Requires running Ceph cluster
        let store = RadosBuilder::from_env()
            .build()
            .expect("Failed to create RADOS store");

        let prefix = Path::from("test/");

        // Put some test objects
        for i in 0..3 {
            let path = Path::from(format!("test/object{}.txt", i));
            store
                .put(&path, format!("data{}", i).into_bytes().into())
                .await
                .expect("Failed to put object");
        }

        // List objects
        let mut stream = store.list(Some(&prefix));
        let mut count = 0;

        while let Some(result) = stream.next().await {
            let _meta = result.expect("Failed to list object");
            count += 1;
        }

        assert!(count >= 3);

        // Clean up
        for i in 0..3 {
            let path = Path::from(format!("test/object{}.txt", i));
            store.delete(&path).await.ok();
        }
    }
}
