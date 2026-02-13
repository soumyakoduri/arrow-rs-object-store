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

fn main() {
    #[cfg(feature = "unified-sal")]
    build_unified_sal();
}

#[cfg(feature = "unified-sal")]
fn build_unified_sal() {
    use std::env;
    use std::path::PathBuf;

    // Ceph source location
    let ceph_src = "../ceph/src";
    let unified_header = format!("{}/include/rgw/rgw_sal_unified.h", ceph_src);
    let unified_impl = format!("{}/rgw/rgw_sal_unified.cc", ceph_src);

    println!("cargo:rerun-if-changed={}", unified_header);
    println!("cargo:rerun-if-changed={}", unified_impl);

    // Compile the unified C API from Ceph repo
    cc::Build::new()
        .cpp(true)
        .file(&unified_impl)
        .flag("-std=c++17")
        .include("/usr/include/ceph")
        .include("/usr/include")
        .include(format!("{}/include", ceph_src)) // Ceph include directory
        .warnings(false) // Suppress warnings from Ceph headers
        .compile("sal_unified");

    // Link against librados and librgw
    println!("cargo:rustc-link-lib=rados");
    println!("cargo:rustc-link-lib=rgw");
    println!("cargo:rustc-link-lib=stdc++");

    // Add library search paths
    if let Ok(ceph_lib_dir) = env::var("CEPH_LIB_DIR") {
        println!("cargo:rustc-link-search=native={}", ceph_lib_dir);
    } else {
        // Common Ceph library locations
        println!("cargo:rustc-link-search=native=/usr/lib");
        println!("cargo:rustc-link-search=native=/usr/lib64");
        println!("cargo:rustc-link-search=native=/usr/local/lib");
        println!("cargo:rustc-link-search=native=/usr/local/lib64");
    }
}

#[cfg(feature = "unified-sal")]
mod build_dependencies_unified {
    // This ensures cc crate is available when unified-sal feature is enabled
    extern crate cc;
}
