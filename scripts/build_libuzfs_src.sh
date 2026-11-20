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
BUILD_STAMP=${INSTALL_DIR}/.libuzfs-build-stamp

CFLAGS="-fPIC -O2 -g -fno-omit-frame-pointer"
if [ "${ENABLE_DEBUG}" = "yes" ]; then
    CFLAGS="-fPIC -fno-omit-frame-pointer"
fi;

zfs_state_hash() {
	{
		git -C "${ZFS_DIR}" rev-parse HEAD 2>/dev/null || true
		git -C "${ZFS_DIR}" diff --binary 2>/dev/null || true
		git -C "${ZFS_DIR}" diff --cached --binary 2>/dev/null || true
		git -C "${ZFS_DIR}" status --porcelain --untracked-files=all 2>/dev/null || true
	} | sha256sum | cut -d' ' -f1
}

BUILD_KEY=$(printf '%s\n' \
	"enable_debug=${ENABLE_DEBUG}" \
	"enable_asan=${ENABLE_ASAN}" \
	"cflags=${CFLAGS}" \
	"zfs_state=$(zfs_state_hash)")

build_libuzfs_lib() {
    cd ${ZFS_DIR}
    ./autogen.sh && CFLAGS=${CFLAGS} ./configure --with-config=user --enable-shared=no --enable-debuginfo=${ENABLE_DEBUG} --enable-debug=${ENABLE_DEBUG} --enable-asan=${ENABLE_ASAN} --prefix=${INSTALL_DIR} && make clean && make gitrev
    cd lib
    make -j && make install
    cd ../include
    make && make install
    cd ${TOP_SRCDIR}
}

setup_libuzfs_pc() {
    cp ${LIBUZFS_PC} ${TOP_SRCDIR}/libuzfs.pc
}


[ -f libuzfs.pc ] && [ -f ${LIBUZFS} ] && [ -f ${BUILD_STAMP} ] && \
	[ "$(cat "${BUILD_STAMP}")" = "${BUILD_KEY}" ] && {
    exit 0
}

git submodule sync --recursive
build_libuzfs_lib
setup_libuzfs_pc
printf '%s\n' "${BUILD_KEY}" > "${BUILD_STAMP}"
