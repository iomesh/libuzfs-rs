use pkg_config;
use project_root;
use std::env;

fn main() {
    let root = project_root::get_project_root().unwrap();
    env::set_var("PKG_CONFIG_PATH", root);
    let mut lib = pkg_config::probe_library("libuzfs").unwrap();
    let link = lib.link_paths.pop().unwrap();
    println!("cargo:rustc-link-search=native={}", link.to_string_lossy());

    let mut ld_path = env::var("LD_LIBRARY_PATH").unwrap();
    ld_path.push_str(":");
    ld_path.push_str(&link.to_string_lossy());
    println!("cargo:rustc-env=LD_LIBRARY_PATH={}", ld_path);
}
