target:
	RUSTFLAGS="-C target-cpu=native" cargo build --release

image:
	docker build . -t distanteagle16/rinhabackend