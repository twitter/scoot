# Scoot Daemon Protocol

The protocol betweeen Scoot Local Server and Scoot Local Clients.
This includes the protocol buffers, and any other utilities that should be shared by client and server.
There is an official daemon client library provided in Python on Pypi.

__This still needs to be integrated into the build__

### 3rd Party Prerequisites:
Install *protobuf*, (python's) *grpcio* and *docopt*.  See scoot/README for instructions.

### Installing from Scoot Pypi

Prerequisites:
* pip

```sh
pip install scoot
```

## Accessing the Daemon API
### client_lib.py
a library for connecting to the Scoot Daemon and issuing commands to it.

### scoot.py
a command line tool for submitting commands to the Scoot Daemon.

---
## Development Instructions:
### Publishing to Scoot Pypi

Prerequisites:
* Register at __https://pypi.python.org__ and then create ~/.pypirc with:
```
    [server-login]
    username:USER
    password:PASS
```
* Request someone on scoot-team to add you as an owner

###### Publishing
* Bump the version in daemon/protocol/python/setup.py
* Run:
```sh
daemon/protocol/python/deploy.sh
```

#### Compiling for Go (daemon.pb.go)

* Install a protoc with libproto version >= 3.0.0 from https://github.com/google/protobuf
* install protoc-gen-go from https://github.com/golang/protobuf (you need this to be on your $PATH, not your GOPATH)
* In this directory (github.com/scootdev/scoot/daemon/protocol), run:
```sh
protoc -I . daemon.proto --go_out=plugins=grpc:.
```

