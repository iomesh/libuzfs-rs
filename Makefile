all: build

build:
	cargo build --all

test:
	cargo test --workspace  -- --nocapture

clean:
	cargo clean
	make -C uzfs-sys clean
