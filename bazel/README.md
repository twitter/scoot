# Bazel Remote Execution for Scoot

This contains initial server support for the Bazel gRPC Remote Execution API.
This consists of two distinct concepts, the Execution API and ContentAddressableStore (CAS) API.

The Bazel API Design Document can be found at: https://docs.google.com/document/d/1AaGk7fOPByEvpAbqeXIyE8HX_A3_axxNnvroblTZ_6s/edit#

### Components:
* cas/ contains CAS API server implementation
* execution/ contains Execution API server implementation
* server/ contains gRPC server abstractions

### Running/testing the API:
* Scheduler will initialize and serve the Execution API over gRPC on the default port.
* Apiserver will initialize and serve the CAS API over gRPC on the default port.
* A (very limited) test binary (binaries/bazelapi) can be built to send client requests against the Scheduler running the Execute API.

Hello world test: Build/install the scheduler, and bazelapi, then run the scheduler and in a separate terminal, the bazelapi binary:
```sh
go install github.com/twitter/scoot/binaries/scheduler github.com/twitter/scoot/binaries/bazelapi
./scheduler
./bazelapi
```

*bazelapi is currently intended as a development tool and not a production client binary*

The Apiserver does not currently have a development/test client.
