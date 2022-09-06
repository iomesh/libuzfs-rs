#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(clippy::redundant_static_lifetimes)]
#![allow(deref_nullptr)]

include!("bindings.rs");

pub const DMU_OT_NEWTYPE: u32 = 0x80;
pub const DMU_OT_BYTESWAP_MASK: u32 = 0x1f;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uzfs_sys_works() {
        unsafe { libuzfs_init() };
        unsafe { libuzfs_fini() };
    }
}
