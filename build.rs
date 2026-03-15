// Build script for arrow-rs-object-store
// This script configures linking to a locally built libceph from the ceph repository

use std::env;
use std::path::PathBuf;

// XXX: examine why this build directory is needed

fn main() {
    // Only run this build script when the 'rgw' feature is enabled
    if !cfg!(feature = "rgw") {
        return;
    }

    // Get the ceph repository path from environment variable or use default
    // Users can set CEPH_PATH environment variable to point to their ceph repo
    let ceph_path = env::var("CEPH_PATH")
        .unwrap_or_else(|_| {
            // Default path relative to this project
            let default_path = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
                .parent()
                .expect("Failed to get parent directory")
                .join("ceph");
            default_path.to_str().unwrap().to_string()
        });

    println!("cargo:warning=Using ceph repository at: {}", ceph_path);

    // Set up include paths for C headers
    let include_path = PathBuf::from(&ceph_path).join("src/include");
    println!("cargo:include={}", include_path.display());

    // Set up library search paths
    let lib_path = PathBuf::from(&ceph_path).join("build/lib");
    println!("cargo:rustc-link-search=native={}", lib_path.display());

    // Add runtime library path (rpath) so the binary can find the libraries at runtime
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_path.display());

    // Link against required ceph libraries
    // Note: The order matters - libraries that depend on others should come first

    // Link the main RGW libraries
    // rgw_common contains the SAL wrapper (rgw_sal_c_wrapper.cc)
    println!("cargo:rustc-link-lib=static=rgw_common");
    println!("cargo:rustc-link-lib=static=rgw_a");

    // Link core ceph libraries
    println!("cargo:rustc-link-lib=dylib=ceph-common");
    println!("cargo:rustc-link-lib=dylib=rados");

    // Link RGW class client libraries
    println!("cargo:rustc-link-lib=static=cls_rgw_client");

    // Link other required system and ceph libraries
    // These are transitive dependencies of the above libraries
    println!("cargo:rustc-link-lib=dylib=stdc++");  // C++ standard library
    println!("cargo:rustc-link-lib=dylib=boost_system");
    println!("cargo:rustc-link-lib=dylib=boost_thread");
    println!("cargo:rustc-link-lib=dylib=boost_context");
    println!("cargo:rustc-link-lib=dylib=fmt");

    // OpenSSL libraries (required by rgw_sal_c_wrapper for ETag calculation)
    println!("cargo:rustc-link-lib=dylib=crypto");  // libcrypto for EVP_md5, etc.
    println!("cargo:rustc-link-lib=dylib=ssl");     // libssl

    // Rerun this build script if the ceph libraries change
    println!("cargo:rerun-if-changed={}/build/lib", ceph_path);
    println!("cargo:rerun-if-changed={}/src/include/rgw/rgw_sal_c.h", ceph_path);
    println!("cargo:rerun-if-env-changed=CEPH_PATH");
}
