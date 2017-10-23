# Bazel Remote Execution for Scoot

This contains initial server support for the Bazel gRPC Remote Execution API.

The Bazel API Design Document can be found at: https://docs.google.com/document/d/1AaGk7fOPByEvpAbqeXIyE8HX_A3_axxNnvroblTZ_6s/edit#

### Running/testing the API:
* The Scheduler, by default, will initialize and serve the API over gRPC on the default port.
* A (very limited) test binary (binaries/bazelapi) can be built to send client requests against the Scheduler running the API.

Build/install the scheduler and bazelapi, then run the scheduler and in a separate terminal, the bazelapi binary:
```sh
go install github.com/twitter/scoot/binaries/scheduler github.com/twitter/scoot/binaries/bazelapi
./scheduler
./bazelapi
```

*bazelapi is currently intended as a development tool and not a production client binary*
