# Scoot
[![Build Status](https://travis-ci.org/twitter/scoot.svg?branch=master)](https://travis-ci.org/twitter/scoot)
[![GoDoc](https://godoc.org/github.com/twitter/scoot?status.svg)](https://godoc.org/github.com/twitter/scoot)
[![codecov.io](https://codecov.io/github/twitter/scoot/coverage.svg?branch=master)](https://codecov.io/gh/twitter/scoot?branch=master)
[![GoReportCard](https://goreportcard.com/badge/github.com/twitter/scoot)](https://goreportcard.com/report/github.com/twitter/scoot)

Scoot is a distributed task runner.
The core concept of Scoot is the Snapshot, which represents an immutable filesystem state.
Scoot allows for execution of commands against input Snapshots to create new output Snapshots.

### Scoot Components

###### Scheduler

The Scheduler receives and distributes jobs to workers, and maintains state. It is also responsible for serving the Cloud Scoot API for clients.

###### Worker

The Worker (workerserver) receives information about jobs and runs them, and is responsible for all Snapshot-related functionality.

###### Client APIs

* Cloud Scoot API
* Worker API

###### Jobs and Tasks

Scoot work is broken down into independent jobs, which can consist of one or more tasks, which are executed sequentially.

#### Go Version

Scoot is intended to work with go modules. If this repository is on your GOPATH, please set **GO111MODULE=on** when
working with go tools in this repository, and preferably use the Go distribution pinned in the .travis.yml file.

### Example

Setup a scheduler and worker nodes locally:

```sh
go run ./binaries/setup-cloud-scoot/main.go --strategy local.local
```

Run a series of randomly generated tests against the local scheduler and workers:

```sh
go run ./binaries/scootapi/main.go run_smoke_test
```
## Scoot Integration Tests
Scoot has a few  tests that exercise varying levels of common usages and workflows.

### Smoketest/Swarmtest
Invokes a scootapi client directly to run jobs against a local cluster and waits for the 
scheduled jobs to complete.

(./scootapi/client/smoke_test_cmd.go)

### Recoverytest
Invokes a scootapi client directly to run jobs against a local cluster, kills the cluster, 
attempts to spin up a new one, and waits for the originally scheduled jobs to complete.

(./binaries/recoverytest/main.go)

### Integration
Invokes a scootapi and scoot-snapshot-db client via CLI to run a job against a local cluster 
and waits for the job to complete

(./tests/integration_test.go)

## Scoot Thrift Code
__Thrift Prerequisites__
Install the Thrift tool and golang thrift repository locally using the following section.

__Generating thrift files (scootapi used as an example)__
See documentation in thrift definition files for specific generation instructions, or `make thrift`.

# Development Installation Instructions
## Install 3rd party tools:
### Thrift (version == 0.9.3)
macOS:
```sh
brew install thrift@0.9
```
### Thrift for go:
go modules will take care of this

## Install/Access Scoot Executables and libraries
Install binaries to $GOPATH/bin:

```sh
make install
```
