#!/usr/bin/env bash

set -e
echo "" > coverage.txt
echo "" > dirlist.out

find . -name \*.go | xargs -n 1 dirname | uniq | grep -v /vendor/ | grep -v /fuse > dirlist.out

for d in $(sort dirlist.out | uniq); do
    go test -timeout 120s -race -coverprofile=profile.out -covermode=atomic $d
    if [ -f profile.out ]; then
        cat profile.out >> coverage.txt
        rm profile.out
    fi
done
rm dirlist.out
