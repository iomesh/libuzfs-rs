use std::{env, fs};

fn main() {
    let root = fs::canonicalize("..").unwrap();
    env::set_var("PKG_CONFIG_PATH", root);
    let mut lib = pkg_config::probe_library("libuzfs").unwrap();
    let link = lib.link_paths.pop().unwrap();
    println!("cargo:rustc-link-search=native={}", link.to_string_lossy());

    let mut ld_path = env::var("LD_LIBRARY_PATH").unwrap();
    ld_path.push(':');
    ld_path.push_str(&link.to_string_lossy());
    println!("cargo:rustc-env=LD_LIBRARY_PATH={}", ld_path);

    // temp workaround: used to pass ld_path to external crate
    println!("cargo:ldpath={}", ld_path);
}
