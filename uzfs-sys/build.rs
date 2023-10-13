use std::{env, fs, process::Command};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/wrapper.h");
    println!("cargo:rerun-if-changed=zfs");

    let root = fs::canonicalize(".").unwrap();

    let enable_debug = if env::var("PROFILE").unwrap() == *"release" {
        "no"
    } else {
        "yes"
    };

    Command::new("make")
        .env("ENABLE_DEBUG", enable_debug)
        .args(["-C", root.to_str().unwrap(), "build_libuzfs_src"])
        .status()
        .expect("failed to make libuzfs");

    env::set_var("PKG_CONFIG_PATH", root);

    // probe_library tells cargo all link info of libuzfs automatically
    let lib = pkg_config::probe_library("libuzfs").unwrap();

    println!("cargo:rustc-link-search=/usr/local/lib");
    println!("cargo:rustc-link-lib=static=minitrace_c");
    println!("cargo:rustc-link-lib=static=minitrace_rust");

    let bindings = bindgen::Builder::default()
        .clang_args(
            lib.include_paths
                .iter()
                .map(|path| format!("-I{}", path.to_string_lossy())),
        )
        .clang_args(
            lib.link_paths
                .iter()
                .map(|path| format!("-L{}", path.to_string_lossy())),
        )
        .clang_args(lib.libs.iter().map(|lib| format!("-l{lib}")))
        .header("src/wrapper.h")
        .allowlist_function("libuzfs_.*")
        .allowlist_var("dmu_ot.*")
        .allowlist_type("DMU_OT.*")
        .derive_default(true)
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file("src/bindings.rs")
        .expect("Couldn't write bindings!");
}
