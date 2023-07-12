#!/bin/sh
# --target=x86_64-unknown-linux-musl
cargo build
cargo build --release
