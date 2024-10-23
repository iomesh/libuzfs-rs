mod dataset;
pub use dataset::*;
pub(crate) mod bindings;
pub(crate) mod context;
pub(crate) mod io;
pub mod metrics;
pub(crate) mod sync;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub struct UzfsTestEnv {
    dev_file: tempfile::NamedTempFile,
}

#[cfg(test)]
impl UzfsTestEnv {
    /// Create an env for uzfs test.
    ///
    /// If dev_size is not zero, this function will create a temp file as block device,
    /// the size of device is `dev_size` bytes.
    ///
    /// If dev_size is zero, no block device and pool/dataset is created
    ///
    /// All of the resources (temp file, dev_file) will be deleted automatically when the env
    /// goes out of scope
    pub fn new(dev_size: u64) -> Self {
        let dev_file = tempfile::NamedTempFile::new().unwrap();

        if dev_size > 0 {
            dev_file.as_file().set_len(dev_size).unwrap();
        }

        UzfsTestEnv { dev_file }
    }

    pub fn get_dev_path(&self) -> &str {
        self.dev_file.path().to_str().unwrap()
    }

    pub fn set_dev_size(&self, new_size: u64) {
        self.dev_file.as_file().set_len(new_size).unwrap();
    }
}
