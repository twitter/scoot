# Scoot
[![Build Status](https://travis-ci.org/scootdev/scoot.svg?branch=master)](https://travis-ci.org/scootdev/scoot)
[![codecov.io](https://codecov.io/github/Kitware/candela/coverage.svg?branch=master)](https://codecov.io/gh/scootdev/scoot?branch=master)
[![GoDoc](https://godoc.org/github.com/scootdev/scoot?status.svg)](https://godoc.org/github.com/scootdev/scoot)

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

__Dependencies for installing thrift__
* Install Thrift manually (version >= 0.9.3). macOS:
```sh
brew install thrift
```

* Thrift for go:
```sh
go get github.com/apache/thrift/lib/go/thrift
```

__Generating thrift files (scootapi used as an example)__
* To Generate files run from this directory:
```sh
thrift --gen go:package_prefix=github.com/scootdev/scoot/scootapi/gen-go/,package=scoot,thrift_import=github.com/apache/thrift/lib/go/thrift scoot.thrift
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

```sh
python -m grpc.tools.protoc -I. --python_out=. --grpc_python_out=. daemon.proto
```

### grpcio for python
```sh
pip install grpcio-tools
```

### docopt for python
```sh
pip install docopt==0.6.2
```
