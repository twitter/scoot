#!/bin/bash
#

set -euo pipefail
trap "exit" INT TERM
trap 'rm -rf ${GOPATH}' EXIT

SCOOT_ORIG="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export GOPATH="$(mktemp -d -t TEMP.XXXXXXX)"

scootdev="$GOPATH/src/github.com/scootdev"
mkdir -p "$scootdev" && cd "$scootdev"
git clone https://github.com/scootdev/scoot

get_deps() {
    cd "$1"
    for need in $(go list -f '{{join .Deps "\n"}}' ./... | \
                  xargs go list -f '{{if not .Standard}}{{.ImportPath}}{{end}}' 2>&1 | \
                  grep "can't load package" | \
                  sed -E 's,[^"]*"([^"]*).*,\1,' | \
                  grep '\..*/'); do
        go get -t -d "$need" || true
        get_deps "$GOPATH/src/$need"
    done
}

echo "Darwin Deps."
export GOOS=darwin GOARCH=amd64
get_deps "$scootdev/scoot"

echo "Windows Deps."
export GOOS=windows GOARCH=amd64
get_deps "$scootdev/scoot"

HANDLED=$(find $GOPATH -name .git | sort | uniq | sed -E "s,$GOPATH/src/|/\.git,,g" | grep -v scootdev)

cd "$SCOOT_ORIG"
for dep in $HANDLED; do
    url=$(cd "$GOPATH/src/$dep" && git config --get remote.origin.url)
    grep "$dep" ".gitmodules" &>/dev/null && continue || echo "Adding $dep"
    git submodule add "$url" "vendor/$dep"
    (cd "vendor/$dep" && git checkout -b scoot)
done
