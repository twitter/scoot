package scootapi

// We should use go generate to run:
// For now to Install Thrift:
//     1. Install Thrift manually `brew install thrift` ensure version is greater that 0.9.3
//     2. go get github.com/apache/thrift/lib/go/thrift
//

// To Generate files run from this directory
//     1. thrift --gen go:package_prefix=github.com/scootdev/scoot/scootapi/gen-go/,package=scoot,thrift_import=github.com/apache/thrift/lib/go/thrift scoot.thrift
