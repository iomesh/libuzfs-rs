#!/bin/bash

set -e

TOP_SRCDIR=$1
ZFS_DIR=${TOP_SRCDIR}/zfs
DOWNLOAD_DIR=${TOP_SRCDIR}/download
LIBUZFS_PC=${TOP_SRCDIR}/libuzfs.pc

rm -rf ${ZFS_DIR} ${DOWNLOAD_DIR} ${LIBUZFS_PC}
