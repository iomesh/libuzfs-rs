#!/bin/bash

set -e

TOP_SRCDIR=$1
ENABLE_DEBUG=$2
ENABLE_ASAN=$3

ZFS_DIR=${TOP_SRCDIR}/zfs
ZFS_TAG=uzfs-1.0.0-rc19
ZFS_PKG=zfs-${ZFS_TAG}
ZFS_ZIP=${ZFS_PKG}.zip
INSTALL_DIR=${TOP_SRCDIR}/install
LIBUZFS=${INSTALL_DIR}/lib/libuzfs.a
DOWNLOAD_DIR=${TOP_SRCDIR}/download
DOWNLOAD_URL=https://github.com/iomesh/zfs/archive/refs/tags/${ZFS_TAG}.zip
LIBUZFS_PC=${INSTALL_DIR}/lib/pkgconfig/libuzfs.pc

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

CFLAGS="-fPIC -O2 -ftls-model=initial-exec -fno-omit-frame-pointer -g"
if [ "${ENABLE_DEBUG}" = "yes" ]; then
    CFLAGS="-fPIC -fno-omit-frame-pointer"
fi;

build_libuzfs_lib() {
    cd ${ZFS_DIR}
    ./autogen.sh && CFLAGS=${CFLAGS} ./configure --with-config=user --enable-shared=no --enable-debuginfo=${ENABLE_DEBUG} --enable-debug=${ENABLE_DEBUG} --enable-asan=${ENABLE_ASAN} --prefix=${INSTALL_DIR} && make gitrev
    cd lib
    make -j4 && make install
    cd ../include
    make && make install
    cd ${TOP_SRCDIR}
}

setup_libuzfs_pc() {
    mv ${LIBUZFS_PC} ${TOP_SRCDIR}/libuzfs.pc
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
