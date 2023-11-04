#!/bin/bash

set -euo pipefail
set -x

ZLIB_PREFIX="${1-/zlib-ng}"

cd /tmp
rm -rf zlib-ng
git clone https://github.com/zlib-ng/zlib-ng.git
cd zlib-ng
git checkout 9fb955b8ba734b6519fe1a35704a4bc0ef01a22d # 2.1.4
CFLAGS="-fPIC -fvisibility=hidden" ./configure --zlib-compat --static --prefix="$ZLIB_PREFIX"
make install
cd ..
rm -rf zlib-ng
