#!/bin/bash

set -e
curl -L https://github.com/bazelbuild/bazelisk/releases/download/v1.16.0/bazelisk-linux-amd64 -o bazel
chmod u+x bazel
./bazel test ...
