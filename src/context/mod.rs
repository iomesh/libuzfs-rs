#[cfg(not(feature = "thread"))]
pub(crate) mod coroutine;
#[cfg(not(feature = "thread"))]
pub(crate) mod coroutine_c;
#[cfg(not(feature = "thread"))]
pub mod stack;
#[cfg(not(feature = "thread"))]
pub(crate) mod taskq;
#[cfg(feature = "thread")]
pub(crate) mod taskq_thread;
#[cfg(feature = "thread")]
pub(crate) mod thread;
#[cfg(feature = "thread")]
pub(crate) mod thread_c;
#[cfg(feature = "thread")]
pub(crate) use thread_c as coroutine_c;
#[cfg(feature = "thread")]
pub(crate) use thread as coroutine;

#[allow(dead_code)]
#[allow(non_upper_case_globals)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(clippy::upper_case_acronyms)]
pub(super) mod libcontext;
