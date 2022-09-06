use pkg_config;
use project_root;
use std::{env, process::Command};

fn main() {
    let root = project_root::get_project_root().unwrap();
    Command::new("make")
        .args(&["-C", root.to_str().unwrap(), "build_libuzfs_src"])
        .status()
        .expect("failed to make libuzfs");
    env::set_var("PKG_CONFIG_PATH", root);
    let mut lib = pkg_config::probe_library("libuzfs").unwrap();
    let link = lib.link_paths.pop().unwrap();

    println!("cargo:rustc-link-search=native={}", link.to_string_lossy());
    println!("cargo:rustc-link-lib=uzfs");

    bindgen::Builder::default()
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
        .expect("Unable to generate bindings")
        .write_to_file("src/bindings.rs")
        .expect("Couldn't write bindings!");

    // set LD_LIBRARY_PATH for cargo run and test
    let mut ld_path = env::var("LD_LIBRARY_PATH").unwrap();
    ld_path.push_str(":");
    ld_path.push_str(&link.to_string_lossy());
    println!("cargo:rustc-env=LD_LIBRARY_PATH={}", ld_path);

    println!("cargo:rerun-if-changed={}", "src/wrapper.h");
}
