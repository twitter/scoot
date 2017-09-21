# Scoot Worker

This contains the Worker API Thrift definition, generated code, and worker
server and client implementations.

We should use go generate to run:
```sh
thrift --gen go:package_prefix=github.com/twitter/scoot/workerapi/gen-go/,thrift_import=github.com/apache/thrift/lib/go/thrift worker.thrift
```
