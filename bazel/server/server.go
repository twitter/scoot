// Bazel Remote Execution API gRPC server
// Wwrapping interface for gRPC servers to work seamlessly with magicbag semantics
package server

import ()

// gRPC server interface encapsulating gRPC operations and execution server,
// intended to reduce gRPC listener and registration boilerplate.
type GRPCServer interface {
	Serve() error
}
