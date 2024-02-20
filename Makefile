all: build

build:
	cargo build --all

test:
	cargo test uzfs_sync_test -- --ignored --nocapture
	cargo test --workspace  -- --nocapture

clean:
	cargo clean
	make -C uzfs-sys clean

bench:
	cargo bench --all
