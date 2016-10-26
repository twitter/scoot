package server

import (
	"net"
	"time"

	"github.com/scootdev/scoot/daemon/protocol"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/snapshot"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Create a protocol.ScootDaemonServer
func NewServer(runner runner.Runner, filer snapshot.Filer) (Server, error) {
	s := &server{
		runner: runner,
		filer:  filer,
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
	filer  snapshot.Filer

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

func (s *server) CreateSnapshot(ctx context.Context, req *protocol.CreateSnapshotRequest) (*protocol.CreateSnapshotReply, error) {
	id, err := s.filer.Ingest(req.Path)
	return &protocol.CreateSnapshotReply{SnapshotId: id, Error: err.Error()}, nil
}

func (s *server) CheckoutSnapshot(ctx context.Context, req *protocol.CheckoutSnapshotRequest) (*protocol.CheckoutSnapshotReply, error) {
	errStr := ""
	co, err := s.filer.Checkout(req.SnapshotId)
	if err == nil {
		err = co.Disown(req.Dir)
	}
	if err != nil {
		errStr = err.Error()
	}
	return &protocol.CheckoutSnapshotReply{Error: errStr}, nil
}

func (s *server) Run(ctx context.Context, req *protocol.RunRequest) (*protocol.RunReply, error) {
	cmd := runner.NewCommand(req.Cmd.Argv, req.Cmd.Env, time.Duration(req.Cmd.TimeoutNs), req.SnapshotId)
	cmd.SnapshotPlan = req.Plan.SrcPathsToDestDirs
	status, err := s.runner.Run(cmd)
	return &protocol.RunReply{RunId: string(status.RunId), Error: err.Error()}, nil
}

const pollInterval = time.Duration(50 * time.Millisecond)

func (s *server) Poll(ctx context.Context, req *protocol.PollRequest) (*protocol.PollReply, error) {
	reply := &protocol.PollReply{}
	callerTimer := &time.Timer{}
	if req.TimeoutNs > 0 {
		callerTimer = time.NewTimer(time.Duration(req.TimeoutNs))
	}

	pollTimer := time.NewTimer(pollInterval)
	if req.TimeoutNs == 0 {
		pollTimer.Stop()
	}

	for {
		select {
		case <-callerTimer.C:
			return reply, nil
		case <-pollTimer.C:
			completed := false
			for runId := range req.RunIds {
				status, _ := s.runner.Status(runner.RunId(runId))
				if status.State.IsDone() {
					completed = true
				}
				if req.All || status.State.IsDone() {
					reply.Status = append(reply.Status, protocol.FromRunnerStatus(status))
				}
			}
			if completed || req.TimeoutNs == 0 {
				return reply, nil
			}
		}
	}
}

//
// TODO: alternate impls to test/benchmark suitability of different protocols/rpcs (ex: Cap'N Proto)
//
