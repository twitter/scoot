// This is where remote grpc server API will live.
// This will get used/imported by scheduler via ice.
// TODO - change protocol to pb per conventions?
package server

import (
	"fmt"
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/twitter/scoot/remote/protocol"
)

// gRPC server interface encapsulating gRPC operations and execution server
type GRPCServer interface {
	Serve() error
}

// Implements GRPCServer and protocol.ExecutionServer interfaces
type executionServer struct {
	listener net.Listener
	server   *grpc.Server
}

func NewExecutionServer(l net.Listener) *executionServer {
	g := executionServer{listener: l, server: grpc.NewServer()}
	protocol.RegisterExecutionServer(g.server, &executionServer{})
	return &g
}

func (s *executionServer) Execute(ctx context.Context, rp *protocol.RequestPlaceholder) (*protocol.ResponsePlaceholder, error) {
	output := fmt.Sprintf("execute! input was: %s", rp.Input)
	r := protocol.ResponsePlaceholder{Output: output}
	return &r, nil
}

func (s *executionServer) Serve() error {
	return s.server.Serve(s.listener)
}
