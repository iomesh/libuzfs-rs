use std::{env, fs};

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

    let root = fs::canonicalize("uzfs-sys").unwrap();

    env::set_var("PKG_CONFIG_PATH", root.clone());
    let mut lib = pkg_config::probe_library("libuzfs").unwrap();
    let link_path = lib.link_paths.pop().unwrap();

    println!("cargo:rustc-env=LD_LIBRARY_PATH={}", link_path.display());
    println!("cargo:rustc-link-search=native={}", link_path.display());
    println!("cargo:rustc-link-lib=uzfs");
    println!("cargo:include={:?}", lib.include_paths);
    println!("cargo:lib={}", link_path.display());
    println!("cargo:root={}", root.display());
}
