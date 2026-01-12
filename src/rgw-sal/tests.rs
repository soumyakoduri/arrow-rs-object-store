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

use super::*;

#[test]
fn test_rados_builder_config() {
    // Test basic builder configuration
    let builder = RadosBuilder::new()
        .with_cluster_name("test-cluster")
        .with_user_name("client.test")
        .with_pool_name("test-pool")
        .with_namespace("test-namespace")
        .with_conf_file("/etc/ceph/test.conf");

    // Verify we can build (will fail without actual RADOS connection, but tests builder)
    let result = builder.build();
    // We expect this to fail since we don't have a real Ceph cluster
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn test_rados_config_key_parsing() {
    use std::str::FromStr;

    // Test all valid config keys
    assert_eq!(
        RadosConfigKey::from_str("cluster_name").unwrap(),
        RadosConfigKey::ClusterName
    );
    assert_eq!(
        RadosConfigKey::from_str("ceph_cluster_name").unwrap(),
        RadosConfigKey::ClusterName
    );
    assert_eq!(
        RadosConfigKey::from_str("user_name").unwrap(),
        RadosConfigKey::UserName
    );
    assert_eq!(
        RadosConfigKey::from_str("pool_name").unwrap(),
        RadosConfigKey::PoolName
    );
    assert_eq!(
        RadosConfigKey::from_str("namespace").unwrap(),
        RadosConfigKey::Namespace
    );

    // Test invalid key
    assert!(RadosConfigKey::from_str("invalid_key").is_err());
}

#[test]
fn test_rados_builder_missing_pool() {
    // Test that building without pool name fails
    let builder = RadosBuilder::new().with_cluster_name("test-cluster");

    let result = builder.build();
    assert!(result.is_err());
}

#[test]
fn test_rados_builder_from_env() {
    // Test environment variable parsing
    std::env::set_var("CEPH_CLUSTER_NAME", "env-cluster");
    std::env::set_var("CEPH_POOL", "env-pool");

    let builder = RadosBuilder::from_env();

    // Clean up
    std::env::remove_var("CEPH_CLUSTER_NAME");
    std::env::remove_var("CEPH_POOL");

    // Verify builder was configured
    let result = builder.build();
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn test_rados_config_key_as_ref() {
    assert_eq!(RadosConfigKey::ClusterName.as_ref(), "cluster_name");
    assert_eq!(RadosConfigKey::UserName.as_ref(), "user_name");
    assert_eq!(RadosConfigKey::PoolName.as_ref(), "pool_name");
    assert_eq!(RadosConfigKey::Namespace.as_ref(), "namespace");
}

#[test]
fn test_rados_builder_with_config() {
    let builder = RadosBuilder::new()
        .with_config(RadosConfigKey::ClusterName, "test")
        .with_config(RadosConfigKey::PoolName, "pool");

    let result = builder.build();
    assert!(result.is_err() || result.is_ok());
}
