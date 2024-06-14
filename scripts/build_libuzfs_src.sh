#!/bin/bash

set -e

TOP_SRCDIR=$1
ENABLE_DEBUG=$2
ENABLE_ASAN=$3

ZFS_DIR=${TOP_SRCDIR}/zfs
ZFS_ZIP=${ZFS_PKG}.zip
INSTALL_DIR=${TOP_SRCDIR}/install
LIBUZFS=${INSTALL_DIR}/lib/libuzfs.a
LIBUZFS_PC=${INSTALL_DIR}/lib/pkgconfig/libuzfs.pc

CFLAGS="-fPIC -O2 -ftls-model=initial-exec -g -fno-omit-frame-pointer"
if [ "${ENABLE_DEBUG}" = "yes" ]; then
    CFLAGS="-fPIC -fno-omit-frame-pointer"
fi;

build_libuzfs_lib() {
    cd ${ZFS_DIR}
    ./autogen.sh && CFLAGS=${CFLAGS} ./configure --with-config=user --enable-shared=no --enable-debuginfo=${ENABLE_DEBUG} --enable-debug=${ENABLE_DEBUG} --enable-asan=${ENABLE_ASAN} --prefix=${INSTALL_DIR} && make gitrev
    cd lib
    make -j && make install
    cd ../include
    make && make install
    cd ${TOP_SRCDIR}
}

setup_libuzfs_pc() {
    cp ${LIBUZFS_PC} ${TOP_SRCDIR}/libuzfs.pc
}


[ -f libuzfs.pc ] && [ -f ${LIBUZFS} ] && {
    exit 0
}

[ -f ${LIBUZFS} ] && {
    setup_libuzfs_pc
    exit 0
}

git submodule sync --recursive
build_libuzfs_lib
setup_libuzfs_pc
