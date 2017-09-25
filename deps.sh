#!/bin/bash -x
# Add new direct and transitive dependencies as submodules.
#
echo "Note: caller must validate/curate/commit changes to .gitmodules and 'vendor/'"
echo "FIXME: may not work for gopkg.in on OSX: 'Unknown SSL protocol error in connection to gopkg.in:-9838'"
echo "       see https://github.com/niemeyer/gopkg/issues/49 for context"
echo '       also try temporarily doing:  'git config --global url."http://".insteadOf https://'

set -euo pipefail
trap "exit" INT TERM
trap 'rm -rf ${GOPATH_NEW}' EXIT

SCOOT_ORIG="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOPATH_NEW="$(mktemp -d -t TEMP.XXXXXXX)"
export GOPATH="${GOPATH_NEW}"

#
get_deps() {
    cd "${GOPATH}/src/$1"
    for need in $(go list -f '{{join .Deps "\n"}}' ./... | \
                  xargs go list -f '{{if not .Standard}}{{.ImportPath}}{{end}}' 2>&1 | \
                  grep "can't load package" | \
                  sed -E 's,[^"]*"([^"]*).*,\1,' | \
                  grep '\..*/'); do
        go get -t -d "${need}" || true
        get_deps "${need}"
    done
}

depRepo="github.com/twitter/scoot"
depDir="${GOPATH}/src/$(dirname ${depRepo})"
mkdir -p "${depDir}" && cd "${depDir}"
cp -R "${SCOOT_ORIG}" "./$(basename ${depRepo})"

echo "Darwin Deps."
export GOOS=darwin GOARCH=amd64
get_deps "${depRepo}"

echo "Windows Deps."
export GOOS=windows GOARCH=amd64
get_deps "${depRepo}"

HANDLED=$(find ${GOPATH} -name .git | sort | uniq | sed -E "s,${GOPATH}/src/|/\.git,,g" | grep -v twitter)

cd "${SCOOT_ORIG}"
for dep in ${HANDLED}; do
    url=$(cd "${GOPATH}/src/${dep}" && git config --get remote.origin.url)
    sha1=$(cd "${GOPATH}/src/${dep}" && git rev-parse HEAD)
    grep "${dep}" ".gitmodules" &>/dev/null && continue || echo "Adding ${dep}"
    git submodule add "${url}" "vendor/${dep}"
    git config -f .gitmodules "submodule.vendor/${dep}.branch" "${sha1}"
done
