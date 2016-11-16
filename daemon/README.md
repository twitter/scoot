# Scoot Daemon Server

The Scoot Daemon will allow use of Scoot's command execution capabilities
both locally or remotely in a Scoot Cloud. The Daemon supports:
* defining environments (snapshots) in which to run commands
* running commands against snapshots
* monitoring and managing runs
* collecting results of runs

The daemon is a local persistent process that has a Protobuf listening
interface, servers the Scoot Daemon API.

### Scoot Daemon API

The API supports the following actions. See the .proto or client files for more info:

* __Echo__ - echo test to verify the daemon is available
* __CreateSnapshot__ - create a snapshot from system files, and get a unique snapshot ID
* __CheckoutSnapshot__ - given a snapshot ID, recreate the file state that makes up the snapshot
* __Run__ - run a command, with options for run environment (snapshot) and output (new snapshot creation)
* __Poll__ - determine the status of run commands

### Scoot Daemon CLI

A Python client that supports some of the above API is available, see the client for details.

