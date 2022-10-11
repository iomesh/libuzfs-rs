all: build

build:
	cargo build --all

test:
	cargo test -- --nocapture

clean:
	cargo clean
