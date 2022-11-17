use std::{env, fs, process::Command};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/buildings.rs");
    println!("cargo:rerun-if-changed=src/wrapper.h");
    println!("cargo:rerun-if-changed=zfs");

    let root = fs::canonicalize(".").unwrap();

    Command::new("make")
        .args(["-C", root.to_str().unwrap(), "build_libuzfs_src"])
        .status()
        .expect("failed to make libuzfs");

    env::set_var("PKG_CONFIG_PATH", root);
    let lib = pkg_config::probe_library("libuzfs").unwrap();

    // "include/libzfs"
    // "include/libspl"
    for inc in &lib.include_paths {
        println!("extra_include: {}", inc.display());
    }

    let bindings = bindgen::Builder::default()
        .clang_args(
            lib.include_paths
                .iter()
                .map(|path| format!("-I{}", path.to_string_lossy())),
        )
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
