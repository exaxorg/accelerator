This directory contains various scripts for building and testing wheels.

You should only need to use build_old_versions.sh and build.sh directly.

# build.sh

Top level build script.
Unlike most of these scripts you should read it and maybe adjust it.
It will download docker images, make new local docker images and build
wheels in these images.

You need to run build_old_versions.sh once before it will work.

## build_prepare.sh

Prepares a docker image for building accelerator wheels. It needs network
access to download some packages.

### build_zlib-ng.sh

Downloads and installs zlib-ng for use in the wheels (statically linked).

## build_wheels.sh

Builds and tests wheels. Does not need network access, as build_prepare.sh
should have already installed everything.

### check_old_versions.sh

Checks that the newly build accelerator can still read old jobs / datasets.

### multiple_interpreters_test.sh

Tests mixing python versions.

# build_old_versions.sh

Produce old_versions.VERSION.BYTEORDER.tar.gz for use in testing.
(Required by build_wheels.sh.)

Needs to run in an old manylinux2010 image, see comment at the top.

## make_old_versions.sh

Used by build_old_versions.py once for each python version.
