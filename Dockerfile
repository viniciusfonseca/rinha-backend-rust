FROM rust:1.71.1

RUN apt-get update -yqq && apt-get install -yqq cmake g++

ADD ./ /actix
WORKDIR /actix

RUN cargo clean
RUN RUSTFLAGS="-C target-cpu=native" cargo build --release

EXPOSE 80

CMD ./target/release/rinha-backend-rust