all: build

build_libuzfs_src:
	@bash ./build_libuzfs_src.sh

clean_libuzfs_src:
	@bash ./clean_libuzfs_src.sh

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
