use std::cell::{RefCell, UnsafeCell};
use std::collections::HashMap;
use std::ptr::null_mut;
use std::{
    io::Error,
    sync::{
        atomic::{AtomicU32, Ordering},
        Mutex,
    },
};

use crossbeam_utils::CachePadded;
use libc::c_void;

static STACK_ID: AtomicU32 = AtomicU32::new(1);
const MAX_STACK_ID: u64 = 256 << 10;
const STACK_ID_MASK: u64 = MAX_STACK_ID - 1;

#[allow(dead_code)]
struct StackBacktrace {
    stack_bottom: *mut c_void,
    stack_top: *mut c_void,
    stack_id: u64,
    lock_contentions: Mutex<Option<HashMap<Vec<u64>, u64>>>,
}

const EMPTY_STACK: CachePadded<StackBacktrace> = CachePadded::new(StackBacktrace {
    stack_bottom: null_mut(),
    stack_top: null_mut(),
    stack_id: 0,
    lock_contentions: Mutex::new(None),
});

#[no_mangle]
static mut STACKS: UnsafeCell<[CachePadded<StackBacktrace>; MAX_STACK_ID as usize]> =
    UnsafeCell::new([EMPTY_STACK; MAX_STACK_ID as usize]);

#[derive(Debug, Clone)]
pub(super) struct Stack {
    pub(super) stack_bottom: *mut c_void,
    // high 48 bit means version, lower 16 bits are id
    pub(super) stack_id: u64,
    stack_size: usize,
}

impl Stack {
    #[cold]
    unsafe fn new(stack_size: usize) -> Result<Self, Error> {
        let page_size = libc::sysconf(libc::_SC_PAGESIZE) as usize;
        assert!(stack_size % page_size == 0);
        let mem = libc::mmap(
            std::ptr::null_mut(),
            stack_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        );

        if mem == libc::MAP_FAILED || libc::mprotect(mem, page_size, libc::PROT_NONE) < 0 {
            Err(Error::last_os_error())
        } else {
            assert!(mem as usize % page_size == 0);
            let stack_id = STACK_ID.fetch_add(1, Ordering::Relaxed) as u64;
            assert!(stack_id < MAX_STACK_ID);
            let stack_bottom = mem.byte_add(stack_size);

            // register in global variable for backtrace
            let stack = &mut STACKS.get_mut()[stack_id as usize];
            stack.stack_bottom = stack_bottom;
            stack.stack_id = stack_id;

            Ok(Self {
                stack_bottom,
                stack_id,
                stack_size,
            })
        }
    }

    #[inline(always)]
    pub(super) unsafe fn record_stack(&self, stack_top: *mut c_void) {
        let idx = self.stack_id & STACK_ID_MASK;
        let stack = &mut STACKS.get_mut()[idx as usize];
        stack.stack_top = stack_top;
        stack.stack_id = self.stack_id;
    }

    #[inline(always)]
    pub(super) unsafe fn remove_stack_record(&self) {
        let idx = self.stack_id & STACK_ID_MASK;
        STACKS.get_mut()[idx as usize].stack_top = null_mut();
    }

    #[inline(always)]
    pub(super) unsafe fn record_lock_contention(&self) {
        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        {
            use std::arch::asm;
            let mut fp: u64;
            #[cfg(target_arch = "x86_64")]
            asm!(
                "mov {fp}, rbp",	// Move the value of rbp (frame pointer) into fp
                fp = out(reg) fp
            );

            #[cfg(target_arch = "aarch64")]
            asm!(
                "mov {0}, x29",		// Move the value of x29 (frame pointer) into fp
                out(reg) fp,
            );

            // there is 2 useless layer, co_mutex_lock and record_lock_contention
            let mut ignore = 2;
            let max_depth = 10;

            let mut bt = Vec::with_capacity(max_depth);
            while fp + 16 != self.stack_bottom as u64 && fp != 0 && bt.len() < max_depth {
                if ignore == 0 {
                    bt.push(*((fp + 8) as *mut u64));
                } else {
                    ignore -= 1;
                }
                fp = *(fp as *const u64);
            }

            let idx = self.stack_id & STACK_ID_MASK;
            let stack = &STACKS.get_mut()[idx as usize];
            let mut contentions = stack.lock_contentions.lock().unwrap();
            match contentions.as_mut() {
                None => *contentions = Some(HashMap::from([(bt, 1)])),
                Some(map) => {
                    if let Some(v) = map.get_mut(&bt) {
                        *v += 1;
                    } else {
                        map.insert(bt, 1);
                    }
                }
            }
        }
    }
}

unsafe impl Send for Stack {}
unsafe impl Sync for Stack {}

// Stack should never be freed
//
// impl Drop for Stack {
//     #[cold]
//     fn drop(&mut self) {
//         assert_eq!(
//             unsafe { libc::munmap(self.stack_bottom.byte_sub(self.stack_size), self.stack_size) },
//             0
//         );
//     }
// }

struct StackPool {
    capacity: usize,
    stacks: Vec<Stack>,
    global: bool,
}

impl StackPool {
    const fn new(capacity: usize, global: bool) -> Self {
        Self {
            capacity,
            stacks: Vec::new(),
            global,
        }
    }

    fn fetch_stack(&mut self) -> Option<Stack> {
        self.stacks.pop()
    }

    fn return_stack(&mut self, mut stack: Stack) -> Result<(), Stack> {
        if self.stacks.len() >= self.capacity {
            Err(stack)
        } else {
            stack.stack_id += MAX_STACK_ID;
            self.stacks.push(stack);
            Ok(())
        }
    }

    fn return_stacks(&mut self, stacks: &mut Vec<Stack>) {
        assert!(self.global);
        assert!(self.stacks.len() + stacks.len() <= self.capacity);
        self.stacks.append(stacks);
    }
}

impl Drop for StackPool {
    fn drop(&mut self) {
        if !self.global {
            GLOBAL_STACK_POOL
                .lock()
                .unwrap()
                .return_stacks(&mut self.stacks);
        }
    }
}

static GLOBAL_STACK_POOL: Mutex<StackPool> =
    Mutex::new(StackPool::new(MAX_STACK_ID as usize, true));

thread_local! {
    static TLS_STACK_POOL: RefCell<StackPool> = const { RefCell::new(StackPool::new(64, false)) };
}

#[inline]
pub(super) fn fetch_or_alloc_stack(stack_size: usize) -> Stack {
    let stack = match TLS_STACK_POOL.with_borrow_mut(|tls_pool| tls_pool.fetch_stack()) {
        Some(stack) => stack,
        None => {
            let mut global_pool = GLOBAL_STACK_POOL.lock().unwrap();
            match global_pool.fetch_stack() {
                Some(stack) => stack,
                None => unsafe { Stack::new(stack_size).unwrap() },
            }
        }
    };

    assert_eq!(stack.stack_size, stack_size);

    stack
}

#[inline]
pub(super) fn return_stack(stack: Stack) {
    let res = TLS_STACK_POOL.with_borrow_mut(|tls_pool| tls_pool.return_stack(stack));
    if let Err(stack) = res {
        let mut global_pool = GLOBAL_STACK_POOL.lock().unwrap();
        global_pool.return_stack(stack).unwrap();
    }
}

#[cold]
pub(super) fn return_stack_to_global(stack: Stack) {
    let mut global_pool = GLOBAL_STACK_POOL.lock().unwrap();
    global_pool.return_stack(stack).unwrap();
}

pub(crate) fn fetch_lock_contentions() -> HashMap<Vec<u64>, u64> {
    let mut contentions = HashMap::with_capacity(128);
    for i in 1..MAX_STACK_ID {
        let stack = unsafe { &STACKS.get_mut()[i as usize] };
        if stack.stack_bottom.is_null() {
            break;
        }
        let map = stack.lock_contentions.lock().unwrap().take();
        if let Some(map) = map {
            for (bt, delta) in map {
                if let Some(count) = contentions.get_mut(&bt) {
                    *count += delta;
                } else {
                    contentions.insert(bt, delta);
                }
            }
        }
    }

    contentions
}
