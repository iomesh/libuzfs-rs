all: build

build:
	cargo build --all

test:
	rustup default nightly-2023-12-28
	export ASAN_OPTIONS=detect_leaks=0 RUSTFLAGS=-Zsanitizer=address RUSTDOCFLAGS=-Zsanitizer=address ENABLE_ASAN=yes
	cargo test --target x86_64-unknown-linux-gnu uzfs_sync_test -- --ignored --nocapture
	cargo test --target x86_64-unknown-linux-gnu --workspace  -- --nocapture

clean:
	cargo clean
	make -C uzfs-sys clean

bench:
	cargo bench --all
