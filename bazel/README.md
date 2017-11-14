# Bazel Remote Execution for Scoot

This contains initial server support for the Bazel gRPC Remote Execution API.
This consists of two distinct concepts, the Execution API and ContentAddressableStore (CAS) API.

The Bazel API Design Document can be found at: https://docs.google.com/document/d/1AaGk7fOPByEvpAbqeXIyE8HX_A3_axxNnvroblTZ_6s/edit#

### Components:
* cas/ contains CAS API server implementation
* execution/ contains Execution API server implementation
* ./ (bazel) contains general Bazel constants, utils, and a gRPC server abstraction

### Running/testing the API:
* Scheduler will initialize and serve the Execution API over gRPC on the default port.
* Apiserver will initialize and serve the CAS API over gRPC on the default port.

```sh
go install github.com/twitter/scoot/binaries/scheduler github.com/twitter/scoot/binaries/apiserver
./scheduler
./apiserver
```

## GRPC CLI Client
Testing the gRPC APIs can be done with the grpc_cli tool, which can be used as a client for any gRPC server.
This has to be built and installed from the grpc repo. See:
https://github.com/grpc/grpc/blob/master/doc/command_line_tool.md

#### Twitter MacBook installation pointers
In general, the grpc install instructions are accurate, but dependency installation via brew might not work.
Use a global, non-MDE copy of brew to install dependencies such as gflags, as MDE brew install will not make
these libraries accessible when trying to compile and link the grpc binaries.
