use dashmap::DashMap;
use libc::{c_char, c_void};
use once_cell::sync::OnceCell;
use std::{ffi::CStr, sync::Arc, time::Duration};

use crate::{
    bindings::async_sys::{libuzfs_show_stats_c, LibuzfsShowStatsArgs},
    context::coroutine::CoroutineFuture,
};

struct StatDescriptor((usize, i32));

static STATS_MAP: OnceCell<DashMap<String, Arc<StatDescriptor>>> = OnceCell::new();

pub(crate) unsafe extern "C" fn install_stat(
    name: *const c_char,
    stat: *mut c_void,
    stat_type: i32,
) {
    let stats_map = STATS_MAP.get_or_init(DashMap::new);
    let name = CStr::from_ptr(name).to_str().unwrap().to_owned();
    stats_map.insert(name, Arc::new(StatDescriptor((stat as usize, stat_type))));
}

pub(crate) unsafe extern "C" fn uninstall_stat(name: *const c_char) {
    let name = CStr::from_ptr(name).to_str().unwrap();
    let stats_map = STATS_MAP.get().unwrap();
    if let Some((_, stat)) = stats_map.remove(name) {
        while Arc::strong_count(&stat) > 1 {
            CoroutineFuture::poll_until_ready(tokio::time::sleep(Duration::from_millis(10)));
        }
    }
}

pub fn list_all_stats() -> Vec<String> {
    if let Some(stats_map) = STATS_MAP.get() {
        stats_map.iter().map(|v| v.key().to_owned()).collect()
    } else {
        Vec::new()
    }
}

pub async fn show_stat(name: &str) -> Option<String> {
    if let Some(stats_map) = STATS_MAP.get() {
        if let Some(v) = stats_map.get(name) {
            let stat = v.value().clone();
            drop(v);
            let (stat_ptr, stat_type) = stat.0;
            let mut arg = LibuzfsShowStatsArgs {
                stat_ptr: stat_ptr as *mut c_void,
                stat_type,
                formatted_strings: Vec::new(),
            };

            let arg_usize = &mut arg as *mut _ as usize;
            CoroutineFuture::new(libuzfs_show_stats_c, arg_usize).await;

            let stats: Vec<_> = arg
                .formatted_strings
                .into_iter()
                .map(|v| {
                    let line = CStr::from_bytes_until_nul(&v).unwrap();
                    line.to_str().unwrap().to_owned()
                })
                .collect();
            return Some(stats.join(""));
        }
    }

    None
}
