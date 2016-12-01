# Scoot Daemon Protocol

The protocol betweeen Scoot Local Server and Scoot Local Clients.
This includes the protocol buffers, and any other utilities that should be shared by client and server.
There is an official daemon client library provided in Python on Pypi.

__This still needs to be integrated into the build__

### Installing from Scoot Pypi

Prerequisites:
* pip

```sh
pip install scoot
```

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

_TODO: replace this with 3rd party deps and code gen instructions in top-level readme_
#### Compiling for Python (daemon_pb2.py)

* Install latest protobuf and grpc+tools packages:
```sh
pip install grpcio-tools
```

* If necessary, remove any conflicting or older versions of Protobuf:
```sh
brew uninstall protobuf
```

```sh
python -m grpc.tools.protoc -I. --python_out=./python/scoot --grpc_python_out=./python/scoot daemon.proto
```

