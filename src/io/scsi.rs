use std::{
    alloc::{alloc_zeroed, dealloc, Layout},
    io::{Error, ErrorKind, Result},
    os::{
        fd::{AsRawFd, RawFd},
        unix::fs::FileTypeExt,
    },
    sync::Arc,
};

use tokio::fs::{File, OpenOptions};

use super::aio::sg_io_hdr;

pub(super) const RECORD_OFFSET: u64 = 256 << 10;
pub(super) const RECORD_SIZE: usize = 512;
pub(super) const MAX_BLOCK_SIZE: usize = 4 << 10;

const SG_IO: libc::c_ulong = 0x2285;
const SG_DXFER_TO_DEV: libc::c_int = -2;
const SG_DXFER_FROM_DEV: libc::c_int = -3;
const SG_INFO_OK_MASK: u32 = 0x1;
const SCSI_READ_10: u8 = 0x28;
const SCSI_WRITE_10: u8 = 0x2a;
const SCSI_INQUIRY: u8 = 0x12;
const SCSI_COMPARE_AND_WRITE_16: u8 = 0x89;
const SCSI_VPD_BLOCK_LIMITS: u8 = 0xb0;
const SCSI_STATUS_CHECK_CONDITION: u8 = 0x02;
const SCSI_SENSE_MISCOMPARE: u8 = 0x0e;

pub(super) struct AlignedScsiBuffer {
    size: usize,
    ptr: *mut u8,
    orig_ptr: *mut u8,
    layout: Layout,
}

impl AlignedScsiBuffer {
    pub(super) fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, MAX_BLOCK_SIZE).unwrap();
        let ptr = unsafe { alloc_zeroed(layout) };

        Self {
            size,
            ptr,
            orig_ptr: ptr,
            layout,
        }
    }

    pub(super) fn slice_mut_from(&mut self, offset: usize, len: usize) -> &mut [u8] {
        debug_assert!(offset < self.size);
        debug_assert!(len <= self.size - offset);
        unsafe { std::slice::from_raw_parts_mut(self.ptr.add(offset), len) }
    }

    pub(super) fn slice_from(&self, offset: usize, len: usize) -> &[u8] {
        debug_assert!(offset < self.size);
        debug_assert!(len <= self.size - offset);
        unsafe { std::slice::from_raw_parts(self.ptr.add(offset), len) }
    }

    fn cut_off(&mut self, offset: usize) {
        debug_assert!(self.size >= offset);
        self.ptr = unsafe { self.ptr.byte_add(offset) };
        self.size -= offset;
    }

    fn ptr_mut(&mut self) -> *mut u8 {
        self.ptr
    }
}

unsafe impl Send for AlignedScsiBuffer {}
unsafe impl Sync for AlignedScsiBuffer {}

impl Drop for AlignedScsiBuffer {
    fn drop(&mut self) {
        unsafe { dealloc(self.orig_ptr, self.layout) };
    }
}

pub(super) struct ScsiDevFile {
    file: Arc<File>,
    pub(super) logical_block_size: usize,
    compare_and_write_supported: bool,
}

impl ScsiDevFile {
    pub(super) async fn new(dev_path: &str) -> Result<Option<Self>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dev_path)
            .await?;

        let meta = file.metadata().await?;
        if !meta.file_type().is_block_device() {
            return Ok(None);
        }

        let mut logical_block_size: libc::c_int = 0;
        let ret =
            unsafe { libc::ioctl(file.as_raw_fd(), libc::BLKSSZGET, &mut logical_block_size) };
        if ret < 0 {
            return Err(Error::last_os_error());
        }

        let logical_block_size = logical_block_size as usize;
        if logical_block_size < RECORD_SIZE
            || !logical_block_size.is_power_of_two()
            || logical_block_size > MAX_BLOCK_SIZE
        {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "device logical block size cannot contain the claim record",
            ));
        }

        let fd = file.as_raw_fd();
        let compare_and_write_supported = Self::query_max_compare_and_write_length(fd, dev_path)?;

        Ok(Some(Self {
            file: Arc::new(file),
            logical_block_size,
            compare_and_write_supported,
        }))
    }

    fn query_max_compare_and_write_length(fd: RawFd, file_path: &str) -> Result<bool> {
        let mut cdb = [0_u8; 16];
        cdb[0] = SCSI_INQUIRY;
        cdb[1] = 0x01; // EVPD
        cdb[2] = SCSI_VPD_BLOCK_LIMITS;
        cdb[4] = 64; // enough for the block limits header and CAW field

        let data = AlignedScsiBuffer::new(64);
        let data = Self::execute_scsi_command(fd, cdb, 6, SG_DXFER_FROM_DEV, data, 64)?;

        let response = data.slice_from(0, 6);
        if response[1] != SCSI_VPD_BLOCK_LIMITS {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "SCSI device returned an unexpected VPD page",
            ));
        }

        let max_compare_and_write_length = response[5];
        println!(
            "SCSI device {file_path}: VPD page {:#04x}, maximum compare and write length: {} blocks, CAW supported: {}",
            SCSI_VPD_BLOCK_LIMITS,
            max_compare_and_write_length,
            max_compare_and_write_length != 0,
        );

        Ok(max_compare_and_write_length != 0)
    }

    fn execute_scsi_command(
        fd: RawFd,
        mut cdb: [u8; 16],
        cmd_len: u8,
        direction: libc::c_int,
        mut data: AlignedScsiBuffer,
        dxfer_len: u32,
    ) -> Result<AlignedScsiBuffer> {
        let mut sense = [0_u8; 64];
        let mut header: sg_io_hdr = unsafe { std::mem::zeroed() };
        header.interface_id = i32::from(b'S');
        header.dxfer_direction = direction;
        header.cmd_len = cmd_len;
        header.mx_sb_len = sense.len() as u8;
        header.dxfer_len = dxfer_len;
        header.dxferp = data.ptr_mut().cast();
        header.cmdp = cdb.as_mut_ptr();
        header.sbp = sense.as_mut_ptr();
        header.timeout = 30_000;

        let ret = unsafe { libc::ioctl(fd, SG_IO, &mut header) };
        if ret < 0 {
            return Err(Error::last_os_error());
        }
        if header.status == 0
            && header.host_status == 0
            && header.driver_status == 0
            && header.info & SG_INFO_OK_MASK == 0
            && header.resid == 0
        {
            return Ok(data);
        }

        let (sense_key, asc, ascq) = match sense[0] & 0x7f {
            0x70 | 0x71 => (sense[2] & 0x0f, sense[12], sense[13]),
            0x72 | 0x73 => (sense[1] & 0x0f, sense[2], sense[3]),
            _ => (0, 0, 0),
        };

        let sense_len = usize::from(header.sb_len_wr).min(sense.len());
        let basic_msg = format!(
            "status={}, host_status={}, driver_status={}, \
		 sense_key={:#x}, asc={:#04x}, ascq={:#04x}, sense={:02x?}, \
		 cdb={:02x?}, dxfer_len={}, resid={}, info={:#x}, flags={:#x}, duration_ms={}",
            header.status,
            header.host_status,
            header.driver_status,
            sense_key,
            asc,
            ascq,
            &sense[..sense_len],
            cdb,
            header.dxfer_len,
            header.resid,
            header.info,
            header.flags,
            header.duration,
        );

        if header.status == SCSI_STATUS_CHECK_CONDITION && sense_key == SCSI_SENSE_MISCOMPARE {
            Err(Error::other(format!(
                "SCSI COMPARE_AND_SWAP failed: {basic_msg}"
            )))
        } else {
            Err(Error::new(
                ErrorKind::Unsupported,
                format!("SCSI command failed: {basic_msg}"),
            ))
        }
    }

    async fn execute_scsi_command_async(
        &self,
        command: u8,
        direction: libc::c_int,
        data: AlignedScsiBuffer,
        dxfer_len: u32,
    ) -> Result<AlignedScsiBuffer> {
        let lba = RECORD_OFFSET / self.logical_block_size as u64;
        let mut cdb = [0_u8; 16];
        cdb[0] = command;
        // cdb[1] flags
        let cmd_len = if command == SCSI_READ_10 || command == SCSI_WRITE_10 {
            let lba = lba as u32;
            cdb[2..6].copy_from_slice(&lba.to_be_bytes());
            cdb[7..9].copy_from_slice(&1_u16.to_be_bytes());
            10
        } else {
            cdb[2..10].copy_from_slice(&lba.to_be_bytes());
            cdb[10..14].copy_from_slice(&1_u32.to_be_bytes());
            16
        };

        let file = self.file.clone();
        tokio::task::spawn_blocking(move || {
            Self::execute_scsi_command(file.as_raw_fd(), cdb, cmd_len, direction, data, dxfer_len)
        })
        .await
        .unwrap()
    }

    // read a single block from the device
    pub(super) async fn sg_read_block(&self) -> Result<AlignedScsiBuffer> {
        let data = AlignedScsiBuffer::new(self.logical_block_size * 2);

        self.execute_scsi_command_async(
            SCSI_READ_10,
            SG_DXFER_FROM_DEV,
            data,
            self.logical_block_size as u32,
        )
        .await
    }

    pub(super) async fn sg_compare_and_write_block(
        &self,
        mut transfer: AlignedScsiBuffer,
    ) -> Result<()> {
        if self.compare_and_write_supported {
            self.execute_scsi_command_async(
                SCSI_COMPARE_AND_WRITE_16,
                SG_DXFER_TO_DEV,
                transfer,
                self.logical_block_size as u32 * 2,
            )
            .await?;
        } else {
            let old = self.sg_read_block().await?;
            {
                let actual = old.slice_from(0, self.logical_block_size);
                let expected = transfer.slice_from(0, self.logical_block_size);
                if actual != expected {
                    return Err(Error::other(format!(
                        "expect: {expected:?}, actual: {actual:?}"
                    )));
                }
            }

            transfer.cut_off(self.logical_block_size);

            self.execute_scsi_command_async(
                SCSI_WRITE_10,
                SG_DXFER_TO_DEV,
                transfer,
                self.logical_block_size as u32,
            )
            .await?;
        }

        Ok(())
    }
}
