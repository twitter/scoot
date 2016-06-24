#!/usr/bin/env bash

set -e
echo "" > coverage.txt

for d in $(find . -name \*.go | xargs -n 1 dirname | uniq | grep -v /vendor/); do
    go test -coverprofile=profile.out -covermode=atomic $d
    if [ -f profile.out ]; then
        cat profile.out >> coverage.txt
        rm profile.out
    fi
done
