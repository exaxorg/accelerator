#!/bin/bash

set -euo pipefail
set -x

ZLIB_PREFIX="${1-/zlib-ng}"

cd /tmp
rm -rf zlib-ng
git clone https://github.com/zlib-ng/zlib-ng.git
cd zlib-ng
git checkout b56a2fd0b126cfe5f13e68ab9090cd4f6a773286 # 2.0.6
CFLAGS="-fPIC -fvisibility=hidden" ./configure --zlib-compat --static --prefix="$ZLIB_PREFIX"
make install
cd ..
rm -rf zlib-ng
