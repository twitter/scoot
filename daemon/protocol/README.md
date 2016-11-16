# Scoot Daemon Protocol

The protocol betweeen Scoot Local Server and Scoot Local Clients.
This includes the protocol buffers, and any other utilities that should be shared by client and server.

__This still needs to be integrated into the build__

#### Compiling for Go (daemon.pb.go)

* Install a protoc with libproto version >= 3.0.0 from https://github.com/google/protobuf
* install protoc-gen-go from https://github.com/golang/protobuf (you need this to be on your $PATH, not your GOPATH)
* In this directory (github.com/scootdev/scoot/daemon/protocol), run:
```sh
protoc -I . daemon.proto --go_out=plugins=grpc:.
```

#### Compiling for Python (daemon_pb2.py)

* Install latest protobuf and grpc+tools packages:
```sh
pip install grpcio-tools
```

_Editor's note_: we should think about using a _virtualenv_ for a fully portable process, and to
prevent potential corruption of system Python installs.

* If necessary, remove any conflicting or older versions of Protobuf:
```sh
brew uninstall protobuf
```

```sh
python -m grpc.tools.protoc -I. --python_out=. --grpc_python_out=. daemon.proto
```
