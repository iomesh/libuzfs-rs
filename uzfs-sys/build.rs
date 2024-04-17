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

    let enable_asan = env::var("ENABLE_ASAN").unwrap_or("no".to_owned());

    Command::new("make")
        .env("ENABLE_DEBUG", enable_debug)
        .env("ENABLE_ASAN", enable_asan)
        .args(["-C", root.to_str().unwrap(), "build_libuzfs_src"])
        .status()
        .expect("failed to make libuzfs");

    env::set_var("PKG_CONFIG_PATH", root);

    // probe_library tells cargo all link info of libuzfs automatically
    let lib = pkg_config::probe_library("libuzfs").unwrap();

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
        .allowlist_function("co_rw_lock_.*")
        .allowlist_function("co_cond_.*")
        .allowlist_function("co_mutex_.*")
        .allowlist_function("coroutine_.*")
        .allowlist_file("coroutine.h")
        .derive_default(true)
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file("src/bindings.rs")
        .expect("Couldn't write bindings!");
}
