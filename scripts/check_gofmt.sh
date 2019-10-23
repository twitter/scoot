#!/bin/bash
set -e

check_fmt() {
    local fmt
    fmt=$(gofmt -l -s .)
    if [[ ! -z "$fmt" ]]; then
        echo "failed gofmt:"
        echo "$fmt"
        exit 1
    fi
}

check_fmt
