# Generating Bazel Remote Execution API Protobuf Code

This directory contains generated and edited code from the Bazel API
definition found at https://github.com/bazelbuild/remote-apis

## Generating Go API Code

### Tools Required

https://grpc.io/docs/quickstart/go.html

* `go get -u google.golang.org/grpc`
* ensure `protoc --version` >= 3
* `go get -u github.com/golang/protobuf/protoc-gen-go`

### Dependencies

Clone the following repositories at the normal place in your GOPATH:

* https://github.com/bazelbuild/remote-apis
* https://github.com/googleapis/googleapis (remote-apis depends on this)

### Generate

We will generate 2 .pb.go files from the remote-apis repository with 2 separate commands,
since these reside in different packages (see https://github.com/golang/protobuf for background on this limitation).

Assumes:
* CWD is the top level of the clone of this scoot repository
* protoc installed at ~/bin/protoc/bin/protoc
* GOPATH is ~/workspace and dependencies are cloned under workspace/src

```sh
~/bin/protoc/bin/protoc \
-I ~/workspace/src/github.com/bazelbuild/remote-apis/ \
-I ~/workspace/src/github.com/googleapis/googleapis/ \
~/workspace/src/github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2/remote_execution.proto \
--go_out=plugins=grpc:bazel/remoteexecution

~/bin/protoc/bin/protoc \
-I ~/workspace/src/github.com/bazelbuild/remote-apis/ \
-I ~/workspace/src/github.com/googleapis/googleapis/ \
~/workspace/src/github.com/bazelbuild/remote-apis/build/bazel/semver/semver.proto \
--go_out=plugins=grpc:bazel/remoteexecution
```

This should generate bazel/remoteexecution/build/...

### Edit (Make it work)

The directory structure and package import paths of the generated code will not work for us. To Fix this,
move the .pb.go to the top level bazel/remoteexecution/ directory:

```sh
mv bazel/remoteexecution/build/bazel/remote/execution/v2/remote_execution.pb.go bazel/remoteexecution/
mv bazel/remoteexecution/build/bazel/semver/semver.pb.go bazel/remoteexecution/
rm -rf bazel/remoteexecution/build
```

Edit the files to normalize them for their current location under the same package:
* change the semver.pb.go package to "remoteexecution"
* remote_execution.pb.go will not need to import "build/bazel/semver" any longer
* remote_execution.pb.go semver package references can be truncated ("semver.S" becomes "S")

Run all tests to verify compatibilty.

#### Other Dependencies

Depending on the proto changes, vendored libraries may need to be updated, e.g.:
* github.com/golang/protobuf
* google.golang.org/grpc

## Usage in the Scoot repo

Use this package for the Bazel API generated code:
```go
import (
    remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
)
```
