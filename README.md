# Scoot
[![Build Status](https://travis-ci.org/scootdev/scoot.svg?branch=master)](https://travis-ci.org/scootdev/scoot)
[![codecov.io](https://codecov.io/github/Kitware/candela/coverage.svg?branch=master)](https://codecov.io/gh/scootdev/scoot?branch=master)
[![GoDoc](https://godoc.org/github.com/scootdev/scoot?status.svg)](https://godoc.org/github.com/scootdev/scoot)

Scoot is infrastructure to make developer tools smaller, simpler, and more distributed. The core concept of Scoot is the Snapshot, which represents an immutable filesystem state. Scoot allows for execution of commands against input Snapshots to create new output Snapshots. [Godoc][godoc].

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

### TBD
Scoot contains many pluggable/overridable interfaces & implementations

