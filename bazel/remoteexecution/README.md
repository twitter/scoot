# Forked Bazel Remote Execution API

This is a local fork of https://github.com/bazelbuild/remote-apis that
allows for temporary divergence from the actual API without managing a forked repo.

## Generating Go API Code

### Tools Required

https://grpc.io/docs/quickstart/go.html

* `go get -u google.golang.org/grpc`
* ensure `protoc --version` >= 3
* `go get -u github.com/golang/protobuf/protoc-gen-go`

### Dependencies

Fetch dependencies required by remote_execution.proto locally.
These are generally googleapis protobuf dependencies defined in
https://github.com/googleapis/googleapis. You can clone this repo locally
or cherry-pick the dependencies (import statements in remote_execution.proto)
to a local directory that mimics the repo's.

### Generate

```sh
[~/workspace/src/github.com/twitter/scoot (user)]$ ~/bin/protoc/bin/protoc \
-I bazel/remoteexecution/ -I ~/workspace/src/github.com/googleapis/googleapis/ \
bazel/remoteexecution/remote_execution.proto --go_out=plugins=grpc:bazel/remoteexecution
```

This should generate bazel/remoteexecution/remote_execution.pb.go

#### Other Dependencies

Depending on the proto changes, vendored libraries may need to be updated, e.g.:
* github.com/golang/protobuf
* google.golang.org/grpc

## Using the Fork

Replace all instances of previous imports of the remoteexecution package:
```go
import (
    remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
)
```

With the local package:
```go
import (
    remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
)
```

Example automated replace:
```sh
grep "remoteexecution \"google\.golang" . -rIl 2>/dev/null | grep -v vendor | xargs sed -i "" 's/google\.golang\.org\/genproto\/googleapis\/devtools\/remoteexecution\/v1test/github\.com\/twitter\/scoot\/bazel\/remoteexecution/g'
```
