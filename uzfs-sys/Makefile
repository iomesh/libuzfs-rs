top_srcdir=`pwd`
scripts_dir=${top_srcdir}/scripts

all: build_libuzfs_src build

build_libuzfs_src:
	@bash ${scripts_dir}/build_libuzfs_src.sh ${top_srcdir} ${ENABLE_DEBUG}

clean_libuzfs_src:
	@bash ${scripts_dir}/clean_libuzfs_src.sh ${top_srcdir}

build_uzfs_sys:
	cargo build -p uzfs_sys

build_uzfs:
	cargo build -p uzfs

build:
	cargo build --all

test:
	cargo test -- --nocapture

clean: clean_libuzfs_src
	cargo clean
