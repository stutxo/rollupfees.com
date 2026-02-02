#!/usr/bin/env bash
set -e

git pull --ff-only origin main
cargo build --release
sudo systemctl restart rollupfees
