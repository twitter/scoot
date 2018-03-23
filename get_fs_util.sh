#!/bin/bash
# Pulls in fs_util tool for use in remote exec ingestion, checkout, and CAS ops
set -e

pants_release="1.6.0.dev0%2B32de7edf"

get_fs_util() {
	case "$(uname -s)" in

		Darwin)
			local url="https://binaries.pantsbuild.org/bin/fs_util/mac/10.10/$pants_release/fs_util"
			echo "Fetching Darwin from $url"
			;;

		Linux)
			local url="https://binaries.pantsbuild.org/bin/fs_util/linux/x86_64/$pants_release/fs_util"
			echo "Fetching Linux from $url"
			;;

		*)
			echo "Expected Darwin or Linux output from uname, received $(uname -s)"
			;;
	esac

	firstgopath=${GOPATH%%:*}

    if [[ -f "$firstgopath/bin/fs_util-$pants_release" ]]; then
        echo "Target file exists locally"
        return
    fi

	wget "$url" -O "$firstgopath/bin/fs_util-$pants_release"
	chmod +x "$firstgopath/bin/fs_util-$pants_release"
    ln -s -f "$firstgopath/bin/fs_util-$pants_release" "$firstgopath/bin/fs_util"
}

get_fs_util
