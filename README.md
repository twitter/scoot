# Scoot
[![Build Status](https://travis-ci.org/twitter/scoot.svg?branch=master)](https://travis-ci.org/twitter/scoot)
[![codecov.io](https://codecov.io/github/Kitware/candela/coverage.svg?branch=master)](https://codecov.io/gh/twitter/scoot?branch=master)
[![GoDoc](https://godoc.org/github.com/twitter/scoot?status.svg)](https://godoc.org/github.com/twitter/scoot)

Scoot is infrastructure to make developer tools smaller, simpler, and more distributed. The core concept of Scoot is the Snapshot, which represents an immutable filesystem state. Scoot allows for execution of commands against input Snapshots to create new output Snapshots.

### Scoot Components

###### Scheduler

The Scheduler receives and distrubutes jobs to workers, and maintains state. It is also responsible for serving the Cloud Scoot API for clients.

###### Worker

The Worker (workerserver) receives information about jobs and runs them, and is responsible for all Snapshot-related functionality.

###### Daemon

The local daemon serves a Protobuf interface and can either send Scoot jobs to a remote scheduler or to embedded local workers.

###### Client APIs

* Cloud Scoot API
* Worker API
* Daemon API

###### Jobs and Tasks

Scoot work is broken down into independent jobs, which can consist of one or more tasks, which are executed sequentially.

### Example

Setup a scheduler and worker nodes locally:

```sh
go run ./binaries/setup-cloud-scoot/main.go --strategy local.local
```

Run a series of randomly generated tests against the local scheduler and workers:

```sh
go run ./binaries/scootapi/main.go run_smoke_test
```

## Scoot Thrift Code
(open source scoot code is in workspace/github.com/twitter/scoot)

__Generating thrift files (scootapi used as an example)__
* To Generate files run from scoot's scootapi directory:
```sh
thrift --gen go:package_prefix=github.com/twitter/scoot/scootapi/gen-go/,package=scoot,thrift_import=github.com/apache/thrift/lib/go/thrift scoot.thrift
```

## Scoot Protobuf Code
__Generating go protobuf files (for local Scoot Daemon)__
cd to scoot's daemon/protocol

protoc -I . daemon.proto --go_out=plugins=grpc:.

__Generating python client files (for client library accessing local Scoot Daemon)__
cd to scoot's daemon/protocol

```sh
python -m grpc.tools.protoc -I. --python_out=./python/scoot --grpc_python_out=./python/scoot daemon.proto
```


# Installation Instructions
## Install 3rd party tools:
###Thrift (version >= 0.9.3)
macOS:
```sh
brew install thrift
```
### Thrift for go:
```sh
go get github.com/apache/thrift/lib/go/thrift
```

### Protobuf for the Scoot Daemon
* If necessary, remove any conflicting or older versions of Protobuf:
```sh
brew uninstall protobuf
```


### grpcio for python
```sh
pip install grpcio-tools
```

### docopt for python
```sh
pip install docopt==0.6.2
```

## Install/Access Scoot Executables and libraries
### Scoot Local Daemon, Local Scheduler and Local Worker
*cd to scoot directory (workspace/github.com/twitter/scoot)
*run: go install ./binaries/...
** the binaries will be installed in workspace/bin

### Scoot Local Daemon Command Line Client
```python workspace/github.com/twitter/scoot/daemon/protocol/python/scoot/scoot.py

### Python client library
Can be found at workspace/github.com/twitter/scoot/daemon/protocol/python/scoot/client_lib.py

