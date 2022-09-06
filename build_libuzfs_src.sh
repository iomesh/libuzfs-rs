#!/bin/bash

ZFS=zfs
ZFS_PKG=zfs-uzfs-dev
ZFS_ZIP=$ZFS_PKG.zip
LIBUZFS=zfs/lib/libuzfs/.libs/libuzfs.so
DOWNLOAD=download
DOWNLOAD_URL=https://github.com/iomesh/zfs/archive/refs/heads/uzfs-dev.zip
LIBUZFS_PC=zfs/lib/libuzfs/libuzfs.pc

download_src() {
    file=$DOWNLOAD/$ZFS_ZIP

    [ -f ${file} ] && unzip -tq ${file} || {
        wget -q -c $DOWNLOAD_URL -O $file || {
            echo "download $file failure"
            exit 1
        }
    }
}

unzip_src() {
    unzip -q $DOWNLOAD/$ZFS_ZIP -d $DOWNLOAD || {
        echo "unzip $ZFS_ZIP failure"
        exit 1
    }

    mv $DOWNLOAD/$ZFS_PKG $ZFS
}

build_libuzfs_lib() {
    root=`pwd`
    cd $ZFS
    ./autogen.sh && ./configure && make gitrev
    cd lib
    make -j4
    cd $root
}

setup_libuzfs_pc() {
    prefix=`readlink -f $ZFS`
    sed "s|/usr/local|${prefix}|g" $LIBUZFS_PC > libuzfs.pc
}


[ -f libuzfs.pc ] && [ -f $LIBUZFS ] && {
    exit 0
}

[ -f $LIBUZFS ] && {
    setup_libuzfs_pc
    exit 0
}

[ ! -d $ZFS ] && [ -f $DOWNLOAD/$ZFS_ZIP ] && {
    unzip_src
    build_libuzfs_lib
    setup_libuzfs_pc
    exit 0
}

rm -rf $ZFS && rm -rf $DOWNLOAD && mkdir $DOWNLOAD

download_src
unzip_src
build_libuzfs_lib
setup_libuzfs_pc

exit 0
