top_srcdir=`pwd`
scripts_dir=${top_srcdir}/scripts

all: build_libuzfs_src build

build_libuzfs_src:
	@bash ${scripts_dir}/build_libuzfs_src.sh ${top_srcdir} ${ENABLE_DEBUG} ${ENABLE_ASAN}

clean_libuzfs_src:
	@bash ${scripts_dir}/clean_libuzfs_src.sh ${top_srcdir}

build_uzfs_sys:
	cargo build -p uzfs_sys

build_uzfs:
	cargo build -p uzfs

build:
	cargo build --all

test:
	rustup default nightly-2023-12-28
	ENABLE_ASAN=yes RUSTFLAGS="-Zsanitizer=address -C force-frame-pointers=yes" ASAN_OPTIONS=detect_leaks=1 \
	  cargo test --target x86_64-unknown-linux-gnu --workspace  -- --nocapture
	ENABLE_ASAN=yes RUSTFLAGS="-Zsanitizer=address -C force-frame-pointers=yes" ASAN_OPTIONS=detect_leaks=0 \
	  cargo test --target x86_64-unknown-linux-gnu uzfs_sync_test -- --ignored --nocapture

clean: clean_libuzfs_src
	cargo clean
