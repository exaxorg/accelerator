#!/bin/bash
# Once you have used build_old_versions.sh this should be the only script
# you need to run.

set -euo pipefail

usage() {
	echo "Usage: $1 ACCELERATOR_BUILD_VERSION commit/tag/branch"
	echo "   or: $1 update"
	echo "   or: $1 update ACCELERATOR_BUILD_VERSION commit/tag/branch"
	exit 1
}

[[ $# -lt 1 || $# -gt 3 ]] && usage "$0"

# These are the containers releases are currently built in.
# You can add or remove architectures as long as the pypa container exists.
# The only constraint is that manylinux2010 is only supported for {x86_64,i686}
# and must come after a modern container of the same architecture.

CONTAINERS=(manylinux_2_28_{x86_64,aarch64} manylinux2014_i686 manylinux2010_{x86_64,i686}:2021-02-06-c17986e {musllinux_1_1,musllinux_1_2}_{x86_64,i686,aarch64})

# Adjust the paths to suit you.
# /out must have old_versions.*.tar.gz and wheelhouse/ in it.
DOCKER_ARGS=(-it -v ~/axbuild:/out:rw -v ~/axbuild/accelerator:/accelerator:ro --tmpfs "/tmp:exec,size=1G")

if [[ "$1" = "update" ]]; then
	for C in "${CONTAINERS[@]}"; do
		docker pull quay.io/pypa/"$C"
		ID="$(docker run --detach "${DOCKER_ARGS[@]}" quay.io/pypa/"$C" /bin/bash)"
		# The 2010 build containers need special handling as they don't have /usr/local/bin/manylinux-entrypoint
		case "$C" in
			manylinux2010_i686*)
				docker exec -it "$ID" /usr/bin/linux32 /accelerator/scripts/build_prepare.sh
				;;
			manylinux2010_x86_64*)
				docker exec -it "$ID" /accelerator/scripts/build_prepare.sh
				;;
			*)
				docker exec -it "$ID" /usr/local/bin/manylinux-entrypoint /accelerator/scripts/build_prepare.sh
				;;
		esac
		docker stop "$ID"
		docker commit "$ID" "axbuild_${C/:*}"
		docker rm "$ID"
	done
	shift;
	[[ $# -eq 0 ]] && exit 0
fi

[[ $# -ne 2 ]] && usage "$0"

for C in "${CONTAINERS[@]}"; do
	docker run --rm --network none "${DOCKER_ARGS[@]}" "axbuild_${C/:*}" /accelerator/scripts/build_wheels.sh "$1" "$2"
done
