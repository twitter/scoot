# Scoot API

Scoot API contains the Scoot Cloud API Thrift definition, Cloud Server code
and client/CLI code for the API.

## Design and Summary
The API itself is defined in scoot.thrift, as service CloudScoot.

##### Server

The server package provides for the implementation of the Cloud API Server. At this time, the server instance is also the collective entry point for the server and scheduler components.
The setup of these is defined by server.Defaults(), which sets up the ice.MagicBag DI and configurations (see scoot/ice). The major components here are setup for:
* Saga Coordinator
* Saga Log
* Scheduler
* API server/handler
* Thrift interface
* Configuration Schema

The actual implementations in scoot/scootapi/server handle Cloud Server API request handling from the Thrift interface down. The main elements here are:
* __MakeHandler__ - main Cloud Scoot API Handler. Implementation here includes scheduler, saga coordinator, and stats receiver.
* __MakeServer__ - wraps the Handler with Thrift connection info and glues the API handler logic to the Thrift interface
* __RunJob__ and __GetStatus__ - API handler implementations

##### Client

The client code in scootapi supports creating a CLI client interface to the Cloud API.  The main interface is __CLIClient__, which defines an Exec function, which will execute a command specified on the command line against the Cloud API.

The __simpleCLIClient__ type implements this interface, and uses github.com/spf13/cobra for command line parsing. The client's communication to Cloud Scoot is done through a Dialer - see scoot/common/dialer.

##### Cloud Scoot Setup

The setup package is a way to initialize a Scoot instance by a strategy, which
defines how the scheduler and workers will be run (locally, in memory, etc).
In a 'local' run, Cloud Setup itself is invoking the other Scoot binaries,
sparing you from running everything independently. This is invoked (setup.Main) with:
* __Cmds__ - exec commands that will be run against the new cloud scoot instance
* __SchedulerStrategy__ - definition of the logic for invoking the scheduler, including WorkersStrategy and Builder

Other Concepts:
* __WorkersStrategy__ - defines the logic for how to invoke the Workers
* __Builder__ - interface that allows specification of how to create the scheduler and worker binaries Cloud Setup will invoke

## ScootAPI Thrift Code

__Dependencies for installing thrift__
* Install Thrift manually (version >= 0.9.3). macOS:
```sh
brew install thrift
```

* Thrift for go:
```sh
go get github.com/apache/thrift/lib/go/thrift
```

__Generating thrift files from this diectory__
* To Generate files run from this directory:
```sh
thrift --gen go:package_prefix=github.com/scootdev/scoot/scootapi/gen-go/,package=scoot,thrift_import=github.com/apache/thrift/lib/go/thrift scoot.thrift
```
