pub(crate) mod coroutine;
pub(crate) mod coroutine_c;
#[allow(static_mut_refs)]
pub(super) mod stack;
pub(crate) mod taskq;

#[allow(dead_code)]
#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(clippy::upper_case_acronyms)]
pub(super) mod libcontext;
