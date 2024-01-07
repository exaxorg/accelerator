#!/bin/bash

set -euo pipefail
set -x

ZLIB_PREFIX="$1"
ZLIB_VERSION="$2"

cd /tmp
rm -rf zlib-ng
git clone https://github.com/zlib-ng/zlib-ng.git
cd zlib-ng
git checkout "$ZLIB_VERSION"
CFLAGS="-fPIC -fvisibility=hidden" ./configure --zlib-compat --static --prefix="$ZLIB_PREFIX"
make install
cd ..
rm -rf zlib-ng
