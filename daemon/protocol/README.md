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
// To upload to your personal pypi account, register at https://pypi.python.org and then create ~/.pypirc with:
//
//   [server-login]
//   username:USER
//   password:PASS
//
// Then run:
//    cd daemon/protocol/python
//    python setup.py sdist upload
//
// To install from pypi:
//    pip install scoot
//
// TODO(jschiller): create a common scootdev account, find out how best to avoid 'scoot' project name conflicts.
