#!/bin/bash

set -euo pipefail
set -x

ZLIB_PREFIX="${1-/zlib-ng}"

cd /tmp
rm -rf zlib-ng
git clone https://github.com/zlib-ng/zlib-ng.git
cd zlib-ng
git checkout cf89cf35037f152ce7adfeca864656de5d33ea1e # 2.1.3
CFLAGS="-fPIC -fvisibility=hidden" ./configure --zlib-compat --static --prefix="$ZLIB_PREFIX"
make install
cd ..
rm -rf zlib-ng
