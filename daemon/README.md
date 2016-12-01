# Scoot Daemon Server

The Scoot Daemon will allow use of Scoot's command execution capabilities
both locally or remotely in a Scoot Cloud. The Daemon supports:
* defining environments (snapshots) in which to run commands
* running commands against snapshots
* monitoring and managing runs
* collecting results of runs

The daemon is a local persistent process that has a Protobuf listening
interface, and serves the Scoot Daemon API.

### Scoot Daemon API

The API supports the following actions. See the .proto or client files for more info:

* __Echo__ - echo test to verify the daemon is available
* __CreateSnapshot__ - create a snapshot from system files, and get a unique snapshot ID
* __CheckoutSnapshot__ - given a snapshot ID, recreate the file state that makes up the snapshot
* __Run__ - run a command, with options for run environment (snapshot) and output (new snapshot creation)
* __Poll__ - determine the status of run commands

TODO: provide installation instructions

# Scoot Daemon CLI
A command line tool that supports submitting API requests to the Scoot Daemon.  See daemon/protocol for installation and access.

# Scoot Daemon Client Library (Python)
A python library for issuing API requests to the Scoot Daemon.  See daemon/protocol for installation and access.
