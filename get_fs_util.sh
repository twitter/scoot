#!/bin/bash
# Pulls in fs_util tool for use in remote exec ingestion, checkout, and CAS ops

pants_release="1.4.0"
fs_util_release="23"

get_fs_util() {

	case "$(uname -s)" in

		Darwin)
			echo 'Darwin'
			local url="https://binaries.pantsbuild.org/bin/fs_util/mac/10.13/$pants_release.dev$fs_util_release/fs_util"
			;;

		Linux)
			echo 'Linux'
			local url="https://binaries.pantsbuild.org/bin/fs_util/mac/10.13/$pants_release.dev$fs_util_release/fs_util"
			;;

		*)
			echo "Expected Darwin or Linux output from uname, received $arch"
			;;

	esac

	wget "$url" -P "$GOPATH/bin/"
	chmod +x "$GOPATH/bin/fs_util"
}

get_fs_util