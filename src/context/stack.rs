use libc::c_void;
use once_cell::sync::OnceCell;
use std::cell::RefCell;
use std::{
    io::Error,
    sync::{
        atomic::{AtomicU32, Ordering},
        Mutex,
    },
};

static STACK_ID: AtomicU32 = AtomicU32::new(1);

pub(crate) struct Stack {
    pub(crate) stack_bottom: *mut c_void,
    // high 32 bit means version, lower 32 bits are id
    pub(crate) stack_id: u64,
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
            Ok(Self {
                stack_bottom: mem.byte_add(stack_size),
                stack_id: (STACK_ID.fetch_add(1, Ordering::Relaxed) as u64) << 32,
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
}

impl StackPool {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            stacks: Vec::with_capacity(capacity),
        }
    }

    fn fetch_stack(&mut self) -> Option<Stack> {
        self.stacks.pop()
    }

    fn return_stack(&mut self, mut stack: Stack) -> Result<(), Stack> {
        if self.stacks.len() >= self.capacity {
            Err(stack)
        } else {
            stack.stack_id += 1;
            self.stacks.push(stack);
            Ok(())
        }
    }
}

static GLOBAL_STACK_POOL: OnceCell<Mutex<StackPool>> = OnceCell::new();

thread_local! {
    static TLS_STACK_POOL: RefCell<StackPool> = RefCell::new(StackPool::new(1000));
}

#[inline]
pub(crate) fn fetch_or_alloc_stack(stack_size: usize) -> Stack {
    let stack = match TLS_STACK_POOL.with_borrow_mut(|tls_pool| tls_pool.fetch_stack()) {
        Some(stack) => stack,
        None => {
            let mut global_pool = GLOBAL_STACK_POOL
                .get_or_init(|| Mutex::new(StackPool::new(10000)))
                .lock()
                .unwrap();
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
pub(crate) fn return_or_release_stack(stack: Stack) {
    let res = TLS_STACK_POOL.with_borrow_mut(|tls_pool| tls_pool.return_stack(stack));
    if let Err(stack) = res {
        let mut global_pool = GLOBAL_STACK_POOL.get().unwrap().lock().unwrap();
        let _ = global_pool.return_stack(stack);
    }
}
