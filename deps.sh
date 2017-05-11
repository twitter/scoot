#!/bin/bash
# Add new direct and transitive dependencies as submodules.
# Note: caller must validate/curate/commit changes to .gitmodules and 'vendor/'
#
echo "Note: caller must validate/curate/commit changes to .gitmodules and 'vendor/'"
echo "DO NOT install dependencies with go get before running this script, this script  will fail to find dependencies previously addedd via go get"

set -euo pipefail
trap "exit" INT TERM
trap 'rm -rf ${GOPATH_NEW}' EXIT

SCOOT_ORIG="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOPATH_ORIG="${GOPATH}"
GOPATH_NEW="$(mktemp -d -t TEMP.XXXXXXX)"
export GOPATH="${GOPATH_NEW}"

get_deps() {
    if [[ -d "${GOPATH}/src/$1" ]]; then
        cd "${GOPATH}/src/$1"
        for need in $(go list -f '{{join .Deps "\n"}}' ./... | \
                      xargs go list -f '{{if not .Standard}}{{.ImportPath}}{{end}}' 2>&1 | \
                      grep "can't load package" | \
                      sed -E 's,[^"]*"([^"]*).*,\1,' | \
                      grep '\..*/'); do
                        if [[ $need == *"github.com/apache/thrift/lib/go/thrift"* ]]; then
                            need="github.com/scootdev/thrift/lib/go/thrift"
                        fi
            go get -t -d "${need}" || true
            get_deps "${need}"
        done
    fi
}

if [[ -z "${DEP_REPO:-}" ]]; then
    DEP_REPO="github.com/scootdev/scoot"
    scootdev="${GOPATH}/src/$(dirname ${DEP_REPO})"
    mkdir -p "${scootdev}" && cd "${scootdev}"
    git clone "https://${DEP_REPO}"
else
    depdir="${GOPATH}/src/$(dirname ${DEP_REPO})"
    mkdir -p "${depdir}" && cd "${depdir}"
    cp -r "${GOPATH_ORIG}/src/${DEP_REPO}" ./
fi

echo "Darwin Deps."
export GOOS=darwin GOARCH=amd64
get_deps "${DEP_REPO}"

echo "Windows Deps."
export GOOS=windows GOARCH=amd64
get_deps "${DEP_REPO}"

HANDLED=$(find ${GOPATH} -name .git | sort | uniq | sed -E "s,${GOPATH}/src/|/\.git,,g" | grep -v scootdev)

cd "${SCOOT_ORIG}"
for dep in ${HANDLED}; do
    url=$(cd "${GOPATH}/src/${dep}" && git config --get remote.origin.url)
    sha1=$(cd "${GOPATH}/src/${dep}" && git rev-parse HEAD)
    grep "${dep}" ".gitmodules" &>/dev/null && continue || echo "Adding ${dep}"
    git submodule add "${url}" "vendor/${dep}"
    git config -f .gitmodules "submodule.vendor/${dep}.branch" "${sha1}"
done
