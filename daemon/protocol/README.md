The protocol betweeen Scoot Local Server and Scoot Local Clients.

This includes the protocol buffers, and any other utilities that should be shared by client and server.

// TODO(dbentley): integrate this into our go build
// For now, to compile into daemon.pb.go, you need to:
// install a protoc with libproto version >= 3.0.0 from https://github.com/google/protobuf
// install protoc-gen-go from https://github.com/golang/protobuf (you need this to be on your $PATH, not your GOPATH)
// in this directory (github.com/scootdev/scoot/daemon/protocol), run:
// protoc -I . daemon.proto --go_out=plugins=grpc:.
//
// To compile into daemon_pb2.py:
//  pip install grpcio-tools # Install latest protobuf and grpc+tools packages.
//  brew uninstall protobuf # Be sure to remove any conflicting/older versions of protobuf.
//  python -m grpc.tools.protoc -I. --python_out=. --grpc_python_out=. daemon.proto
//
