#!/bin/bash
# This is for running in a manylinux2010 docker image, so /bin/bash is fine.
# (or manylinux2014 on s390x)
#
# On little endian systems you must use a manylinux2010 image that has both
# python 2.7 and 3.7.
# Since no such image exists for big endian systems you can use a manylinux2014
# image with just python 3.7.
#
# docker run -it --rm -v /some/where:/out:rw -v /path/to/accelerator:/accelerator:ro --tmpfs /tmp:exec,size=1G quay.io/pypa/manylinux2010_x86_64:2021-02-06-c17986e /accelerator/scripts/build_old_versions.sh
#
# this will produce /out/old_versions.$VERSION.$ENDIANNESS.tar.gz

set -euo pipefail
set -x

test -d /accelerator/.git || exit 1
test -d /accelerator/accelerator || exit 1
test -d /out || exit 1

VERSION=5
ENDIANNESS="$(python -c 'import sys; print(sys.byteorder)')"

test -e "/out/old_versions.$VERSION.$ENDIANNESS.tar.gz" && exit 0

if [ "$ENDIANNESS" = "little" ]; then
	if [ ! -e "/opt/python/cp27-cp27mu/bin/python" ]; then
		echo >&2 "Run in an older image that has python 2.7"
		echo >&2 "e.g. quay.io/pypa/manylinux2010_x86_64:2021-02-06-c17986e"
		exit 1
	fi
	/opt/python/cp27-cp27mu/bin/pip install virtualenv "setproctitle==1.1.8" "bottle==0.12.7" "waitress==1.0" "configparser==3.5.0" "monotonic==1.0" "selectors2==2.0.0"
	PYV="cp27-cp27mu cp37-cp37m"
	VE=/opt/python/cp27-cp27mu/bin/virtualenv
else
	PYV=cp37-cp37m
	VE=""
fi
/opt/python/cp37-cp37m/bin/pip install "setproctitle==1.1.8" "bottle==0.12.7" "waitress==1.0"

/accelerator/scripts/build_zlib-ng.sh

mkdir /tmp/old_versions
cd /tmp/old_versions

# (Don't use ACCELERATOR_BUILD_STATIC_ZLIB, because some old versions don't understand it.)
for V in $PYV; do
	mkdir "old.$V"
	CPPFLAGS="-I/zlib-ng/include" \
	LDFLAGS="-L/zlib-ng/lib" \
	USER="DUMMY" \
	/accelerator/scripts/make_old_versions.sh "/opt/python/$V/bin/python" /accelerator "/tmp/old_versions/old.$V" $VE
	VE=""
done

tar zcf "/out/old_versions.$VERSION.$ENDIANNESS.tar.gz" .

set +x

echo
echo OK
echo
