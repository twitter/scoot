// Package server provides the daemon Server interface and an
// implementation, which includes an API Handler and Protobuf server,
// that fulfils daemon.proto.
package server

import (
	"net"
	"time"

	"github.com/scootdev/scoot/daemon/protocol"
	"github.com/scootdev/scoot/runner"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Create a protocol.ScootDaemonServer.
func NewServer(handler *Handler) (Server, error) {
	s := &daemonServer{
		handler:    handler,
		grpcServer: grpc.NewServer(),
	}
	protocol.RegisterScootDaemonServer(s.grpcServer, s)
	return s, nil
}

type Server interface {
	Listen() (net.Listener, error)
	Serve(net.Listener) error
	ListenAndServe() error
	Stop()
}

type daemonServer struct {
	handler    *Handler
	grpcServer *grpc.Server
}

// Return a net.Listener suitable as input to Serve().
func (s *daemonServer) Listen() (net.Listener, error) {
	return Listen()
}

// Serve serves Scoot Daemon at scootdir
func (s *daemonServer) Serve(l net.Listener) error {
	return s.grpcServer.Serve(l)
}

// Convenience function which can be used if the caller doesn't need to know when Listen() is completes.
func (s *daemonServer) ListenAndServe() error {
	l, err := s.Listen()
	if err != nil {
		return err
	}
	return s.Serve(l)
}

// Stops the daemonServer, canceling all active RPCs
func (s *daemonServer) Stop() {
	s.grpcServer.Stop()
}

func (s *daemonServer) Echo(ctx context.Context, req *protocol.EchoRequest) (*protocol.EchoReply, error) {
	return &protocol.EchoReply{Pong: req.Ping}, nil
}

func (s *daemonServer) CreateSnapshot(ctx context.Context, req *protocol.CreateSnapshotRequest) (*protocol.CreateSnapshotReply, error) {
	if id, err := s.handler.CreateSnapshot(req.Path); err == nil {
		return &protocol.CreateSnapshotReply{SnapshotId: id}, nil
	} else {
		return &protocol.CreateSnapshotReply{SnapshotId: id, Error: err.Error()}, nil
	}
}

func (s *daemonServer) CheckoutSnapshot(ctx context.Context, req *protocol.CheckoutSnapshotRequest) (*protocol.CheckoutSnapshotReply, error) {
	if err := s.handler.CheckoutSnapshot(runner.SnapshotId(req.SnapshotId), req.Dir); err == nil {
		return &protocol.CheckoutSnapshotReply{}, nil
	} else {
		return &protocol.CheckoutSnapshotReply{Error: err.Error()}, nil
	}
}

func (s *daemonServer) Run(ctx context.Context, req *protocol.RunRequest) (*protocol.RunReply, error) {
	cmd := runner.NewCommand(req.Cmd.Argv, req.Cmd.Env, time.Duration(req.Cmd.TimeoutNs), req.Cmd.SnapshotId)
	if status, err := s.handler.Run(cmd); err == nil {
		return &protocol.RunReply{RunId: string(status.RunId)}, nil
	} else {
		return &protocol.RunReply{RunId: string(status.RunId), Error: err.Error()}, nil
	}
}

func (s *daemonServer) Poll(ctx context.Context, req *protocol.PollRequest) (*protocol.PollReply, error) {
	reply := &protocol.PollReply{}

	runIds := []runner.RunId{}
	for _, runId := range req.RunIds {
		runIds = append(runIds, runner.RunId(runId))
	}

	statuses := s.handler.Poll(runIds, time.Duration(req.TimeoutNs), req.All)
	for _, status := range statuses {
		reply.Status = append(reply.Status, protocol.FromRunnerStatus(status))
	}

	return reply, nil
}

func (s *daemonServer) StopDaemon(ctx context.Context, req *protocol.EmptyStruct) (*protocol.EmptyStruct, error) {
	s.grpcServer.Stop()
	return &protocol.EmptyStruct{}, nil
}

//
// TODO: alternate impls to test/benchmark suitability of different protocols/rpcs (ex: Cap'N Proto)
//
