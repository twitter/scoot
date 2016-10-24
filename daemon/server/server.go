package server

import (
	"net"
	"time"

	"github.com/scootdev/scoot/daemon/protocol"
	"github.com/scootdev/scoot/runner"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Create a protocol.ScootDaemonServer
func NewServer(runner runner.Runner) (Server, error) {
	s := &server{
		runner: runner,
		server: grpc.NewServer(),
	}
	protocol.RegisterScootDaemonServer(s.server, s)
	return s, nil
}

type Server interface {
	ListenAndServe() error
	Stop()
}

type server struct {
	runner runner.Runner

	server *grpc.Server
}

func (s *server) ListenAndServe() error {
	l, err := Listen()
	if err != nil {
		return err
	}
	return s.Serve(l)
}

// Serve serves Scoot Daemon at scootdir
func (s *server) Serve(l net.Listener) error {
	return s.server.Serve(l)
}

// Stops the server, canceling all active RPCs
func (s *server) Stop() {
	s.server.Stop()
}

func (s *server) Echo(ctx context.Context, req *protocol.EchoRequest) (*protocol.EchoReply, error) {
	return &protocol.EchoReply{Pong: req.Ping}, nil
}

func (s *server) Run(ctx context.Context, req *protocol.Command) (*protocol.ProcessStatus, error) {
	cmd := runner.NewCommand(req.Argv, req.Env, time.Duration(req.Timeout), "")
	status, err := s.runner.Run(cmd)
	if err != nil {
		return nil, err
	}
	return protocol.FromRunnerStatus(status), nil
}

func (s *server) Status(ctx context.Context, req *protocol.StatusQuery) (*protocol.ProcessStatus, error) {
	status, err := s.runner.Status(runner.RunId(req.RunId))
	if err != nil {
		return nil, err
	}
	return protocol.FromRunnerStatus(status), nil
}

//
// TODO: alternate impls to test/benchmark suitability of different protocols/rpcs (ex: Cap'N Proto)
//
