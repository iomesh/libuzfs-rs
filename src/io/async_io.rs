use std::{
    io::{Error, ErrorKind, Result},
    os::fd::AsRawFd,
    ptr::{null_mut, slice_from_raw_parts},
    sync::{
        atomic::{fence, AtomicPtr, AtomicU32, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
};

use libc::{c_char, c_int, c_void};
use tokio::{
    io::{unix::AsyncFd, Interest},
    task::JoinHandle,
};

use super::aio::*;
use crate::context::coroutine::CoroutineFuture;

type DoneFunc = unsafe extern "C" fn(arg: *mut libc::c_void, res: i64);
type InitArgsFunc =
    unsafe extern "C" fn(*mut c_void, *mut u64, *mut *mut c_char, *mut usize) -> c_int;

#[inline]
unsafe fn io_setup(nr_events: i64) -> Result<aio_context_t> {
    let mut io_ctx: aio_context_t = 0;
    let res = libc::syscall(
        libc::SYS_io_setup,
        nr_events,
        &mut io_ctx as *mut aio_context_t,
    );

    if res == 0 {
        Ok(io_ctx)
    } else {
        Err(Error::last_os_error())
    }
}

#[inline]
unsafe fn io_destroy(io_ctx: aio_context_t) -> Result<()> {
    if libc::syscall(libc::SYS_io_destroy, io_ctx) != 0 {
        Err(Error::last_os_error())
    } else {
        Ok(())
    }
}

#[inline]
unsafe fn io_submit(io_ctx: aio_context_t, iocbs: &[iocb]) -> Result<()> {
    if iocbs.is_empty() {
        return Ok(());
    }

    let iocb_ptrs: Vec<_> = iocbs.iter().map(|iocb| iocb as *const iocb).collect();
    let mut nsubmitted = 0;
    while nsubmitted < iocb_ptrs.len() {
        let ret = libc::syscall(
            libc::SYS_io_submit,
            io_ctx,
            iocbs.len() - nsubmitted,
            iocb_ptrs.as_ptr().add(nsubmitted),
        );
        assert!(ret != 0);
        if ret > 0 {
            nsubmitted += ret as usize;
            continue;
        }

        let err = Error::last_os_error();
        if err.kind() != ErrorKind::Interrupted && err.kind() != ErrorKind::WouldBlock {
            return Err(err);
        }
    }

    Ok(())
}

const SEM_CLOSED: usize = usize::MAX;

struct Semaphore {
    count: Mutex<usize>,
    cond: Condvar,
}

impl Semaphore {
    fn new(count: usize) -> Self {
        Self {
            count: Mutex::new(count),
            cond: Condvar::new(),
        }
    }

    fn post(&self) {
        let mut count = self.count.lock().unwrap();
        *count += 1;
        self.cond.notify_all();
    }

    fn close(&self) {
        let mut count = self.count.lock().unwrap();
        *count = SEM_CLOSED;
        self.cond.notify_all();
    }

    fn acquire_all(&self) -> std::result::Result<(), ()> {
        let mut count = self.count.lock().unwrap();
        loop {
            if *count == SEM_CLOSED {
                return Err(());
            }

            if *count > 0 {
                *count = 0;
                return Ok(());
            }

            count = self.cond.wait(count).unwrap();
        }
    }
}

const AIO_READ: c_int = 0;
const AIO_WRITE: c_int = 1;
const AIO_FSYNC: c_int = 2;

pub(super) struct TaskList {
    head: AtomicPtr<c_void>,
    sem: Semaphore,
    next_off: usize,
    init_io_arg: InitArgsFunc,
    io_fd: i32,
    eventfd: i32,
}

impl TaskList {
    pub(super) fn new_arc(
        next_off: usize,
        init_io_arg: InitArgsFunc,
        io_fd: i32,
        eventfd: i32,
    ) -> Arc<Self> {
        Arc::new(Self {
            head: AtomicPtr::new(null_mut()),
            sem: Semaphore::new(0),
            next_off,
            init_io_arg,
            io_fd,
            eventfd,
        })
    }

    #[inline]
    pub(super) unsafe fn push(&self, arg: *mut c_void) {
        let next = arg.byte_add(self.next_off) as *mut *mut c_void;

        // Fetch the current head and update it with the new task node
        // `Ordering::Release` ensures that the writes to the task node (like `(*task).next`)
        // happen before updating the head pointer.
        // This prevents other threads from seeing an incomplete task node with an uninitialized `next`.
        let was_empty = self
            .head
            .fetch_update(Ordering::Release, Ordering::Relaxed, |ptr| {
                *next = ptr;
                Some(arg)
            })
            .unwrap()
            .is_null();

        if was_empty {
            self.sem.post();
        }
    }

    fn close(&self) {
        self.sem.close();
    }

    #[inline]
    unsafe fn pop_all(&self) -> Option<Vec<iocb>> {
        if self.sem.acquire_all().is_err() {
            return None;
        }

        // Use `swap` with `Ordering::Acquire` to read and clear the head pointer.
        // `Ordering::Acquire` ensures that all operations before this load (such as the writes to the task list)
        // are visible to this thread. Without this, we might see stale data or incomplete updates to the list.
        let mut cur = self.head.swap(null_mut(), Ordering::Acquire);

        let mut res = Vec::with_capacity(256);
        while !cur.is_null() {
            let mut off = 0;
            let mut data = null_mut();
            let mut len = 0;
            let io_type = (self.init_io_arg)(cur, &mut off, &mut data, &mut len);

            let opcode = match io_type {
                AIO_READ => IOCB_CMD_PREAD,
                AIO_WRITE => IOCB_CMD_PWRITE,
                AIO_FSYNC => IOCB_CMD_FSYNC,
                _ => unimplemented!(),
            };

            res.push(iocb {
                aio_data: cur as u64,
                aio_key: 0,
                aio_rw_flags: 0,
                aio_lio_opcode: opcode as u16,
                aio_reqprio: 0,
                aio_fildes: self.io_fd as u32,
                aio_buf: data as u64,
                aio_nbytes: len as u64,
                aio_offset: off as i64,
                aio_reserved2: 0,
                aio_flags: IOCB_FLAG_RESFD,
                aio_resfd: self.eventfd as u32,
            });

            cur = *(cur.byte_add(self.next_off) as *mut *mut c_void);
        }

        Some(res)
    }
}

impl Drop for TaskList {
    fn drop(&mut self) {
        assert!(self.head.load(Ordering::Relaxed).is_null());
    }
}

unsafe impl Send for TaskList {}
unsafe impl Sync for TaskList {}

struct Eventfd(i32);

impl Eventfd {
    fn new() -> Result<Self> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd >= 0 {
            Ok(Self(fd))
        } else {
            Err(Error::last_os_error())
        }
    }
}

impl Drop for Eventfd {
    fn drop(&mut self) {
        unsafe { libc::close(self.0) };
    }
}

impl AsRawFd for Eventfd {
    fn as_raw_fd(&self) -> std::os::unix::prelude::RawFd {
        self.0
    }
}

struct IoCompletions {
    completions: Vec<io_event>,
    io_done: DoneFunc,
}

unsafe impl Send for IoCompletions {}
unsafe impl Sync for IoCompletions {}

unsafe extern "C" fn process_completion(arg: *mut libc::c_void) {
    let completions = &*(arg as *const IoCompletions);
    for completion in &completions.completions {
        (completions.io_done)(completion.data as *mut libc::c_void, completion.res);
    }
}

#[repr(C)]
struct AioRing {
    id: u32,
    nr: u32,
    head: u32,
    tail: u32,
    magic: u32,
    compat_features: u32,
    incompat_features: u32,
    header_length: u32,
}

const AIO_RING_MAGIC: u32 = 0xa10a10a1;

struct AioReaper {
    io_ctx: aio_context_t,
    eventfd: AsyncFd<Eventfd>,
    io_done: DoneFunc,
}

impl AioReaper {
    async fn reap(self) {
        let aio_ring = unsafe { &mut *(self.io_ctx as *mut AioRing) };
        let events_slice = unsafe {
            &*slice_from_raw_parts(
                (self.io_ctx as usize + size_of::<AioRing>()) as *const io_event,
                aio_ring.nr as usize,
            )
        };
	let atomic_tail = unsafe { AtomicU32::from_ptr(&mut aio_ring.tail) };
	let atomic_head = unsafe { AtomicU32::from_ptr(&mut aio_ring.head) };

        loop {
            self.eventfd.readable().await.unwrap().clear_ready();
            assert_eq!(aio_ring.magic, AIO_RING_MAGIC);

            let mut events = Vec::with_capacity(MAX_EVENTS);
            loop {
                let head = aio_ring.head;
                if head == atomic_tail.load(Ordering::Acquire) {
                    break;
                }

                events.push(events_slice[head as usize]);
                atomic_head.store((head + 1) % aio_ring.nr, Ordering::Release);
            }

            if !events.is_empty() {
                let mut completions = IoCompletions {
                    completions: events,
                    io_done: self.io_done,
                };

                let arg_usize = &mut completions as *mut _ as usize;
                CoroutineFuture::new(process_completion, arg_usize).await;
            }
        }
    }
}

pub(super) struct AioContext {
    pub(super) task_list: Arc<TaskList>,
    reaper: JoinHandle<()>,
    submitter: thread::JoinHandle<()>,
    io_ctx: aio_context_t,
}

const MAX_EVENTS: usize = 4096;

impl AioContext {
    fn submit(task_list: Arc<TaskList>, io_ctx: aio_context_t) {
        while let Some(tasks) = unsafe { task_list.pop_all() } {
            unsafe { io_submit(io_ctx, &tasks).unwrap() };
        }
    }

    pub(super) fn start(
        io_fd: i32,
        io_done: DoneFunc,
        next_off: usize,
        init_io_args: InitArgsFunc,
    ) -> Result<Self> {
        let io_ctx = unsafe { io_setup(MAX_EVENTS as i64)? };
        let eventfd = AsyncFd::with_interest(Eventfd::new()?, Interest::READABLE)?;
        let task_list = TaskList::new_arc(next_off, init_io_args, io_fd, eventfd.as_raw_fd());
        let task_list_cloned = task_list.clone();
        let submitter = std::thread::spawn(move || Self::submit(task_list_cloned, io_ctx));

        let reaper = AioReaper {
            io_ctx,
            eventfd,
            io_done,
        };
        let reaper = tokio::spawn(reaper.reap());

        Ok(Self {
            task_list,
            reaper,
            submitter,
            io_ctx,
        })
    }

    pub(super) async fn exit(self) {
        self.task_list.close();
        fence(Ordering::SeqCst);
        self.submitter.join().unwrap();
        self.reaper.abort();
        self.reaper.await.unwrap_err();
        unsafe { io_destroy(self.io_ctx).unwrap() }
    }
}
