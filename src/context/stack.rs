use libc::c_void;
use std::cell::RefCell;
use std::{
    io::Error,
    sync::{
        atomic::{AtomicU32, Ordering},
        Mutex,
    },
};

static STACK_ID: AtomicU32 = AtomicU32::new(1);
const MAX_STACK_ID: u64 = 256 << 10;

#[derive(Debug)]
pub(super) struct Stack {
    pub(super) stack_bottom: *mut c_void,
    // high 46 bit means version, lower 18 bits are id
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
            Ok(Self {
                stack_bottom: mem.byte_add(stack_size),
                stack_id,
                stack_size,
            })
        }
    }
}

unsafe impl Send for Stack {}
unsafe impl Sync for Stack {}

impl Drop for Stack {
    #[cold]
    fn drop(&mut self) {
        assert_eq!(
            unsafe { libc::munmap(self.stack_bottom.byte_sub(self.stack_size), self.stack_size) },
            0
        );
    }
}

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

    fn return_stack(&mut self, mut stack: Stack, id_increment: u64) -> Result<(), Stack> {
        if self.stacks.len() >= self.capacity {
            Err(stack)
        } else {
            stack.stack_id += id_increment;
            self.stacks.push(stack);
            Ok(())
        }
    }

    fn return_multi(&mut self, stacks: &mut Vec<Stack>) {
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
                .return_multi(&mut self.stacks);
        }
    }
}

static GLOBAL_STACK_POOL: Mutex<StackPool> =
    Mutex::new(StackPool::new(MAX_STACK_ID as usize, true));

thread_local! {
    static TLS_STACK_POOL: RefCell<StackPool> = const { RefCell::new(StackPool::new(128, false)) };
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
pub(super) fn return_stack(stack: Stack, inc_id: bool) {
    let id_increment = inc_id as u64 * MAX_STACK_ID;
    let res = TLS_STACK_POOL.with_borrow_mut(|tls_pool| tls_pool.return_stack(stack, id_increment));
    if let Err(stack) = res {
        let mut global_pool = GLOBAL_STACK_POOL.lock().unwrap();
        global_pool.return_stack(stack, id_increment).unwrap();
    }
}

#[cold]
pub(super) fn return_stack_to_global(stack: Stack, inc_id: bool) {
    let id_increment = inc_id as u64 * MAX_STACK_ID;
    let mut global_pool = GLOBAL_STACK_POOL.lock().unwrap();
    global_pool.return_stack(stack, id_increment).unwrap();
}
