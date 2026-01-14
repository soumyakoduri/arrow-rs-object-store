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
    #[cfg(feature = "rgw-sal")]
    build_rgw_sal();
}

#[cfg(feature = "rgw-sal")]
fn build_rgw_sal() {
    use std::env;
    use std::path::PathBuf;

    println!("cargo:rerun-if-changed=cpp/rgw_sal_wrapper.cpp");

    // Compile the C++ wrapper
    cc::Build::new()
        .cpp(true)
        .file("cpp/rgw_sal_wrapper.cpp")
        .flag("-std=c++17")
        .include("/usr/include/ceph")
        .include("/usr/include")
        .warnings(false) // Suppress warnings from Ceph headers
        .compile("rgw_sal_wrapper");

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

#[cfg(feature = "rgw-sal")]
mod build_dependencies {
    // This ensures cc crate is available when rgw-sal feature is enabled
    extern crate cc;
}
