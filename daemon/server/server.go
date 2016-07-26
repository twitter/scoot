package server

import (
	"github.com/scootdev/scoot/daemon/protocol"
	"github.com/scootdev/scoot/runner"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
	"time"
)

// Create a protocol.ScootDaemonServer
func NewServer(runner runner.Runner) (*Server, error) {
	s := &Server{
		runner: runner,
		server: grpc.NewServer(),
	}
	protocol.RegisterScootDaemonServer(s.server, s)
	return s, nil
}

type Server struct {
	runner runner.Runner

	server *grpc.Server
}

func (s *Server) ListenAndServe() error {
	l, err := Listen()
	if err != nil {
		return err
	}
	return s.Serve(l)
}

// Serve serves Scoot Daemon at scootdir
func (s *Server) Serve(l net.Listener) error {
	return s.server.Serve(l)
}

// Stops the server, canceling all active RPCs
func (s *Server) Stop() {
	s.server.Stop()
}

func (s *Server) Echo(ctx context.Context, req *protocol.EchoRequest) (*protocol.EchoReply, error) {
	return &protocol.EchoReply{Pong: req.Ping}, nil
}

func (s *Server) Run(ctx context.Context, req *protocol.Command) (*protocol.ProcessStatus, error) {
	cmd := runner.NewCommand(req.Argv, req.Env, time.Duration(req.Timeout))
	status, err := s.runner.Run(cmd)
	if err != nil {
		return nil, err
	}
	return protocol.FromRunnerStatus(status), nil
}

func (s *Server) Status(ctx context.Context, req *protocol.StatusQuery) (*protocol.ProcessStatus, error) {
	status, err := s.runner.Status(runner.RunId(req.RunId))
	if err != nil {
		return nil, err
	}

	return protocol.FromRunnerStatus(status), nil
}
