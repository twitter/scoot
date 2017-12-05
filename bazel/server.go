// Bazel Remote Execution API gRPC
package bazel

import (
	"net"
)

// Wrapping interface for gRPC servers to work seamlessly with magicbag semantics

// gRPC server interface encapsulating gRPC operations and execution server,
// intended to reduce gRPC listener and registration boilerplate.
type GRPCServer interface {
	Serve() error
}

// Type alias for clarity in use for certain listeners
type GRPCListener net.Listener
