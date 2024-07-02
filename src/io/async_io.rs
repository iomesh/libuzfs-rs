use super::{aio::*, eventfd::EventFd};
use crate::context::coroutine::AsyncCoroutine;
use std::io::{Error, ErrorKind};
use tokio::sync::mpsc::*;

#[inline]
unsafe fn io_setup(nr_events: i64) -> Result<aio_context_t, Error> {
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
unsafe fn io_destroy(io_ctx: aio_context_t) -> Result<(), Error> {
    if libc::syscall(libc::SYS_io_destroy, io_ctx) != 0 {
        Err(Error::last_os_error())
    } else {
        Ok(())
    }
}

#[inline]
unsafe fn io_submit(io_ctx: aio_context_t, iocbs: &[iocb]) -> Result<(), Error> {
    let iocb_ptrs: Vec<_> = iocbs.iter().map(|iocb| iocb as *const iocb).collect();
    let mut nsubmitted = 0;
    while nsubmitted < iocb_ptrs.len() {
        let ret = libc::syscall(
            libc::SYS_io_submit,
            io_ctx,
            iocb_ptrs.len() - nsubmitted,
            iocb_ptrs.as_ptr().add(nsubmitted),
        );
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

#[inline]
unsafe fn io_getevents(io_ctx: aio_context_t, ncompleted: usize) -> Result<Vec<io_event>, Error> {
    let mut events: Vec<io_event> = Vec::with_capacity(ncompleted);
    let mut nreaped = 0;
    while nreaped < ncompleted {
        let ret = libc::syscall(
            libc::SYS_io_getevents,
            io_ctx,
            1,
            (ncompleted - nreaped) as u64,
            events.as_mut_ptr().add(nreaped),
            std::ptr::null_mut::<libc::timespec>(),
        );

        if ret > 0 {
            nreaped += ret as usize;
            continue;
        }

        let err = Error::last_os_error();
        if err.kind() != ErrorKind::Interrupted && err.kind() != ErrorKind::WouldBlock {
            return Err(err);
        }
    }

    events.set_len(ncompleted);
    Ok(events)
}

#[derive(Default)]
pub(super) struct IoContent {
    pub(super) data_ptr: usize,
    pub(super) offset: u64,
    pub(super) size: u64,
}

pub(super) enum IoType {
    Read,
    Write,
    Sync,
}

pub(super) struct AioCallback {
    pub(super) io_type: IoType,
    pub(super) io_content: IoContent,
    pub(super) arg: *mut libc::c_void,
}

unsafe impl Send for AioCallback {}
unsafe impl Sync for AioCallback {}

struct IoCompletions {
    completions: Vec<io_event>,
    cb: unsafe extern "C" fn(arg: *mut libc::c_void, res: i64),
}

unsafe impl Send for IoCompletions {}
unsafe impl Sync for IoCompletions {}

unsafe extern "C" fn process_completion(arg: *mut libc::c_void) {
    let completions = &*(arg as *const IoCompletions);
    for completion in &completions.completions {
        (completions.cb)(completion.data as *mut libc::c_void, completion.res);
    }
}

pub(super) struct AioContex {
    io_fd: i32,
    io_ctx: aio_context_t,
    eventfd: EventFd,
    nr_requests: u32,
    inflight_reqs: u32,
    receiver: Option<UnboundedReceiver<AioCallback>>,
    cb: unsafe extern "C" fn(arg: *mut libc::c_void, res: i64),
}

impl AioContex {
    pub(super) fn new(
        io_fd: i32,
        nr_requests: u32,
        receiver: UnboundedReceiver<AioCallback>,
        cb: unsafe extern "C" fn(arg: *mut libc::c_void, res: i64),
    ) -> Result<Self, Error> {
        let io_ctx = unsafe { io_setup(nr_requests as i64)? };
        let eventfd = EventFd::new(0)?;
        Ok(Self {
            io_fd,
            io_ctx,
            eventfd,
            nr_requests,
            inflight_reqs: 0,
            receiver: Some(receiver),
            cb,
        })
    }

    pub(super) fn submit_tasks(&mut self, tasks: Vec<AioCallback>) {
        self.inflight_reqs += tasks.len() as u32;
        let iocbs: Vec<iocb> = tasks
            .into_iter()
            .map(|aio_cb| {
                let opcode = match aio_cb.io_type {
                    IoType::Read => IOCB_CMD_PREAD,
                    IoType::Write => IOCB_CMD_PWRITE,
                    IoType::Sync => IOCB_CMD_FSYNC,
                };

                iocb {
                    aio_data: aio_cb.arg as u64,
                    aio_key: 0,
                    aio_rw_flags: 0,
                    aio_lio_opcode: opcode as u16,
                    aio_reqprio: 0,
                    aio_fildes: self.io_fd as u32,
                    aio_buf: aio_cb.io_content.data_ptr as u64,
                    aio_nbytes: aio_cb.io_content.size,
                    aio_offset: aio_cb.io_content.offset as i64,
                    aio_reserved2: 0,
                    aio_flags: IOCB_FLAG_RESFD,
                    aio_resfd: self.eventfd.raw_fd() as u32,
                }
            })
            .collect();
        unsafe { io_submit(self.io_ctx, &iocbs).unwrap() };
    }

    pub(super) async fn reap_tasks(&mut self, ncompleted: u64) {
        let completions = unsafe { io_getevents(self.io_ctx, ncompleted as usize).unwrap() };
        assert_eq!(ncompleted, completions.len() as u64);
        self.inflight_reqs -= completions.len() as u32;
        let mut completions = IoCompletions {
            completions,
            cb: self.cb,
        };
        let arg = &mut completions as *mut _ as usize;
        AsyncCoroutine::new(process_completion, arg, true).await;
    }

    pub(super) async fn recv_many(
        receiver: &mut UnboundedReceiver<AioCallback>,
        limit: usize,
    ) -> Result<Vec<AioCallback>, ()> {
        let mut tasks = Vec::with_capacity(limit);
        let nrecv = receiver.recv_many(&mut tasks, limit).await;
        if nrecv == 0 {
            Err(())
        } else {
            Ok(tasks)
        }
    }

    pub(super) async fn submit_and_reap_tasks(&mut self) -> Result<(), ()> {
        let mut receiver = self.receiver.take().unwrap();
        loop {
            let limit = self.nr_requests - self.inflight_reqs;
            if self.inflight_reqs == 0 {
                let tasks = Self::recv_many(&mut receiver, limit as usize).await?;
                self.submit_tasks(tasks);
            } else if self.inflight_reqs < self.nr_requests {
                tokio::select! {
                    tasks = Self::recv_many(&mut receiver, limit as usize) => {
                        self.submit_tasks(tasks?);
                    }
                    ncompleted = self.eventfd.read_events() => {
                        self.reap_tasks(ncompleted.unwrap()).await;
                    }
                }
            } else {
                let ncompleted = self.eventfd.read_events().await.unwrap();
                self.reap_tasks(ncompleted).await;
            }
        }
    }
}

impl Drop for AioContex {
    fn drop(&mut self) {
        unsafe { io_destroy(self.io_ctx).unwrap() }
    }
}
