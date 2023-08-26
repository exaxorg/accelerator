#!/bin/bash
# This is for running in a manylinux or similar docker image, so /bin/bash is fine.
#
# docker run -it --rm -v /some/where:/out:rw -v /path/to/accelerator:/accelerator:ro --tmpfs /tmp:exec,size=1G quay.io/pypa/manylinux2014_x86_64 /accelerator/scripts/build_wheels.sh 20xx.xx.xx.dev1 commit/tag/branch
# docker run -it --rm -v /some/where:/out:rw -v /path/to/accelerator:/accelerator:ro --tmpfs /tmp:exec,size=1G quay.io/pypa/manylinux2010_x86_64:2021-02-06-c17986e /accelerator/scripts/build_wheels.sh 20xx.xx.xx.dev1 commit/tag/branch
#
# or preferably:
# docker run --rm --network none -v /some/where:/out:rw -v /path/to/accelerator:/accelerator:ro --tmpfs /tmp:exec,size=1G YOUR_DOCKER_IMAGE_YOU_HAVE_RUN_build_prepare.sh /accelerator/scripts/build_wheels.sh 20xx.xx.xx.dev1 commit/tag/branch
#
# if you run it in an image where you have already run build_prepare.sh you can run it with --network none

set -euo pipefail

if [ -z "${AUDITWHEEL_ARCH-}" ]; then
	echo "Run in a recent manylinux2014 container."
	exit 1
fi

if [[ "${AUDITWHEEL_POLICY-}" =~ manylinux_2_ ]]; then
	# You can build in a PEP 600 container, but the wheels should still be manylinux2014
	export AUDITWHEEL_POLICY=manylinux2014
	export AUDITWHEEL_PLAT=manylinux2014_"$AUDITWHEEL_ARCH"
fi

X86glibc="false"
if [ "$AUDITWHEEL_ARCH" = "x86_64" -o "$AUDITWHEEL_ARCH" = "i686" ]; then
	if [ "${AUDITWHEEL_PLAT/%20*}" = "manylinux" ]; then
		X86glibc="true"
	fi
fi

if [ "$#" != "2" ]; then
	echo "Usage: $0 ACCELERATOR_BUILD_VERSION commit/tag/branch"
	echo
	if [ "$X86glibc" = "true" ]; then
		echo "Run first in a recent manylinux2014 container,"
		echo "then in manylinux2010:2021-02-06-c17986e or earlier."
	else
		echo "Run in a recent manylinux2014 container."
	fi
	exit 1
fi

set -x
shopt -s extglob

test -d /out/wheelhouse || exit 1
test -d /accelerator/.git || exit 1
test -d /accelerator/accelerator || exit 1

if [ "$0" != "/tmp/accelerator/scripts/build_wheels.sh" ]; then
	cd /tmp
	rm -rf accelerator
	git clone -ns /accelerator
	cd accelerator
	git checkout "$2"
	exec /tmp/accelerator/scripts/build_wheels.sh "$@"
fi

case "$1" in
	20[2-9][0-9].[01][0-9].[0-3][0-9])
		ACCELERATOR_BUILD=IS_RELEASE
		;;
	20[2-9][0-9].[01][0-9].[0-3][0-9].@(dev|rc)[1-9])
		ACCELERATOR_BUILD=DEV
		;;
	*)
		echo "Specify a valid ACCELERATOR_BUILD_VERSION please"
		exit 1
		;;
esac
VERSION="$1"
NAME="accelerator-${VERSION//.0/.}"

BUILT=()

/tmp/accelerator/scripts/build_prepare.sh

MANYLINUX_VERSION="${AUDITWHEEL_PLAT/%_*}"
WHEEL_WILDCARD="${MANYLINUX_VERSION/%20*}*_$AUDITWHEEL_ARCH"

# For manylinux all sorts of stuff happens with the tag, but for musllinux
# we can match on the complete platform tag without messing around.
if [ "$MANYLINUX_VERSION" = "musllinux" ]; then
	WHEEL_WILDCARD="$AUDITWHEEL_PLAT"
fi

ZLIB_PREFIX="/prepare/zlib-ng"

if [ "$MANYLINUX_VERSION" = "manylinux2010" ]; then
	BUILD_STEP="old"
	VERSIONS=(/opt/python/cp[23][5-7]-*)
	if [ ! -e "/out/wheelhouse/$NAME-cp310-cp310-"$WHEEL_WILDCARD.whl ]; then
		echo "First build in a manylinux2014 container"
		exit 1
	fi
	# The 2010 wheels are in our case 1-compatible
	AUDITWHEEL_PLAT="manylinux1_$AUDITWHEEL_ARCH"
else
	BUILD_STEP="new"
	if [ "$X86glibc" = "true" ]; then
		VERSIONS=(/opt/python/cp3[89]-* /opt/python/cp31[0-9]-*)
	else
		VERSIONS=(/opt/python/cp3[5-9]-* /opt/python/cp31[0-9]-*)
	fi
fi


SDIST="/out/wheelhouse/$NAME.tar.gz"
if [ -e "$SDIST" ]; then
	BUILT_SDIST=""
	mkdir /tmp/sdist_check
	cd /tmp/sdist_check
	tar zxf "$SDIST"
	SDIST_COMMIT="$(cat /tmp/sdist_check/*/accelerator/version.txt | tail -1)"
	cd /tmp/accelerator
	rm -rf /tmp/sdist_check
	REPO_COMMIT="$(git rev-parse HEAD)"
	if [ "$SDIST_COMMIT" != "$REPO_COMMIT" ]; then
		set +x
		echo
		echo "Attempting to build $REPO_COMMIT"
		echo "but $SDIST"
		echo "is built from $SDIST_COMMIT"
		exit 1
	fi
else
	cd /tmp/accelerator
	ACCELERATOR_BUILD_VERSION="$VERSION" ACCELERATOR_BUILD="$ACCELERATOR_BUILD" /opt/python/cp38-cp38/bin/python3 ./setup.py sdist
	cp -p "dist/$NAME.tar.gz" /tmp/
	SDIST="/tmp/$NAME.tar.gz"
	BUILT_SDIST="$SDIST"
fi


cd /tmp
rm -rf /tmp/wheels
mkdir /tmp/wheels /tmp/wheels/fixed

build_one_wheel() {
	set -euo pipefail
	set -x
	ACCELERATOR_BUILD_STATIC_ZLIB="$ZLIB_PREFIX/lib/libz.a" \
	CPPFLAGS="-I$ZLIB_PREFIX/include" \
	"/opt/python/$V/bin/pip" wheel "$SDIST" --no-deps -w /tmp/wheels/
	auditwheel repair "$UNFIXED_NAME" -w /tmp/wheels/fixed/
	"/opt/python/$V/bin/pip" install "/tmp/wheels/fixed/$NAME-$V-"$WHEEL_WILDCARD.whl
}

Vs=()

# build all in parallel
# error checking suffers, so we check that no ax is installed before
for V in "${VERSIONS[@]}"; do
	V="${V/\/opt\/python\//}"
	test -e "/opt/python/$V/bin/ax" && exit 1
	UNFIXED_NAME="/tmp/wheels/$NAME-$V-linux_$AUDITWHEEL_ARCH.whl"
	test -e "/out/wheelhouse/$NAME-$V-"$WHEEL_WILDCARD.whl && continue
	rm -f "$UNFIXED_NAME" "/tmp/wheels/fixed/$NAME-$V-"$WHEEL_WILDCARD.whl
	build_one_wheel &
	Vs+=("$V")
done

wait # for all builds to finish

for V in "${Vs[@]}"; do
	if [ ! -e "/opt/python/$V/bin/ax" ]; then
		echo "build failed on (at least) $V"
		exit 1
	fi
done


test_one() {
	set -euo pipefail
	set -x
	V="$1"
	SLICES="$2"
	TEST_DIR="/tmp/ax test.$V.$SLICES"
	rm -rf "$TEST_DIR"
	TEST_NAME="${V/*-/}"
	if [[ "$V" =~ cp3.* ]]; then
		TEST_NAME="Ⅲ $TEST_NAME"
	else
		TEST_NAME="2 $TEST_NAME"
	fi
	"/opt/python/$V/bin/ax" init --slices "$SLICES" --name "$TEST_NAME" "$TEST_DIR" $3
	"/opt/python/$V/bin/ax" --config "$TEST_DIR/accelerator.conf" server &
	SERVER_PID=$!
	trap 'test -n "$SERVER_PID" && kill $SERVER_PID' EXIT
	sleep 1
	"/opt/python/$V/bin/ax" --config "$TEST_DIR/accelerator.conf" run tests
	kill "$SERVER_PID"
	SERVER_PID=""
	rm -rf "$TEST_DIR"
	# verify that we can still read old datasets
	for SRCDIR in /prepare/old.*; do
		PATH="/opt/python/$V/bin:$PATH" /tmp/accelerator/scripts/check_old_versions.sh "$SRCDIR"
	done
	touch "/tmp/ax.$V.OK"
}

for V in "${Vs[@]}"; do
	rm -f "/tmp/ax.$V.OK"
	if [ "$V" = "cp27-cp27m" -o "$V" = "cp38-cp38" ]; then
		# run one test per major python version with extra slices and TCP
		# (must specify localhost IP for some docker reason)
		SLICES=7
		EXTRA="--tcp 127.0.0.1"
	else
		# run the other tests with the lowest (and fastest) number of slices
		# the tests work with, over the default unix sockets
		SLICES=3
		EXTRA=""
	fi
	test_one "$V" "$SLICES" "$EXTRA" 2>&1 | tee "/tmp/output.$V" &
done

wait # for all tests to finish

for V in "${Vs[@]}"; do
	if [ ! -e "/tmp/ax.$V.OK" ]; then
		set +x
		echo
		echo "*** Tests failed on $V ***"
		echo "tail of output (/tmp/output.$V):"
		echo
		tail -64 "/tmp/output.$V"
		echo
		echo "*** Tests failed on $V ***"
		exit 1
	fi
	# The wheel passed the tests, copy it to the wheelhouse (later).
	BUILT+=("/tmp/wheels/fixed/$NAME-$V-"$WHEEL_WILDCARD.whl)
done


if [ "$BUILD_STEP" = "old" ]; then
	# Test running 2.7 and 3.5 under a 3.7 server
	/tmp/accelerator/scripts/multiple_interpreters_test.sh \
		/opt/python/cp37-cp37m/bin \
		/opt/python/cp27-cp27mu/bin \
		/opt/python/cp35-cp35m/bin

	# Test running 3.6 and 3.5 under a 2.7 server
	/tmp/accelerator/scripts/multiple_interpreters_test.sh \
		/opt/python/cp27-cp27m/bin \
		/opt/python/cp36-cp36m/bin \
		/opt/python/cp35-cp35m/bin
elif [ "$X86glibc" = "false" ]; then
	# Test running 3.10 and 3.6 under a 3.8 server
	/tmp/accelerator/scripts/multiple_interpreters_test.sh \
		/opt/python/cp38-cp38/bin \
		/opt/python/cp310-cp310/bin \
		/opt/python/cp36-cp36m/bin
else # X86glibc new
	# Test running 3.12 and 3.8 under a 3.10 server
	/tmp/accelerator/scripts/multiple_interpreters_test.sh \
		/opt/python/cp310-cp310/bin \
		/opt/python/cp312-cp312/bin \
		/opt/python/cp38-cp38/bin
fi


# finally copy everything to /out/wheelhouse
for N in "${BUILT[@]}"; do
	cp -p "$N" /out/wheelhouse/
done
if [ -n "$BUILT_SDIST" ]; then
	cp -p "$BUILT_SDIST" /out/wheelhouse/
	BUILT+=("$BUILT_SDIST")
fi


set +x

echo
echo
echo "Built the following files:"
for N in "${BUILT[@]}"; do
	echo "${N/*\//}"
done
if [ "$BUILD_STEP" = "new" -a "$X86glibc" = "true" ]; then
	echo
	echo "Remember to also build in an old manylinux2010 container."
fi
