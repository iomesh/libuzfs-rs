#!/bin/bash

set -e

TOP_SRCDIR=$1

ZFS_DIR=${TOP_SRCDIR}/zfs
ZFS_PKG=zfs-uzfs-dev
ZFS_ZIP=${ZFS_PKG}.zip
LIBUZFS=${TOP_SRCDIR}/zfs/lib/libuzfs/.libs/libuzfs.so
DOWNLOAD_DIR=${TOP_SRCDIR}/download
DOWNLOAD_URL=https://github.com/iomesh/zfs/archive/refs/heads/uzfs-dev.zip
LIBUZFS_PC=${TOP_SRCDIR}/zfs/lib/libuzfs/libuzfs.pc

download_src() {
    file=${DOWNLOAD_DIR}/${ZFS_ZIP}

    [ -f ${file} ] && unzip -tq ${file} || {
        wget -q -c ${DOWNLOAD_URL} -O $file || {
            echo "download $file failure"
            exit 1
        }
    }
}

unzip_src() {
    unzip -q ${DOWNLOAD_DIR}/${ZFS_ZIP} -d ${DOWNLOAD_DIR} || {
        echo "unzip ${ZFS_ZIP} failure"
        exit 1
    }

    mv ${DOWNLOAD_DIR}/${ZFS_PKG} ${ZFS_DIR}
}

build_libuzfs_lib() {
    cd ${ZFS_DIR}
    ./autogen.sh && ./configure && make gitrev
    cd lib
    make -j4
    cd ${TOP_SRCDIR}
}

setup_libuzfs_pc() {
    sed "s|/usr/local|${ZFS_DIR}|g" ${LIBUZFS_PC} > libuzfs.pc
}


[ -f libuzfs.pc ] && [ -f ${LIBUZFS} ] && {
    exit 0
}

[ -f ${LIBUZFS} ] && {
    setup_libuzfs_pc
    exit 0
}

[ ! -d ${ZFS_DIR} ] && [ -f ${DOWNLOAD_DIR}/${ZFS_ZIP} ] && {
    unzip_src
    build_libuzfs_lib
    setup_libuzfs_pc
    exit 0
}

rm -rf ${ZFS_DIR} && rm -rf ${DOWNLOAD_DIR} && mkdir ${DOWNLOAD_DIR}

download_src
unzip_src
build_libuzfs_lib
setup_libuzfs_pc
