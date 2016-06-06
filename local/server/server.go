package server

import (
	"github.com/scootdev/scoot/local/protocol"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
)

// Create a protocol.LocalScootServer
func NewServer() (*Server, error) {
	return &Server{}, nil
}

type Server struct {
}

// TODO(dbentley): how to cancel

// Serve  serves the Scoot instance in scootdir with logic handler s.
func Serve(s *Server, scootdir string) error {
	socketPath := protocol.SocketForDir(scootdir)
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		return err
	}
	defer l.Close()
	server := grpc.NewServer()
	protocol.RegisterLocalScootServer(server, s)
	server.Serve(l)
	return nil
}

func (s *Server) Echo(ctx context.Context, req *protocol.EchoRequest) (*protocol.EchoReply, error) {
	return &protocol.EchoReply{Pong: "Pong: " + req.Ping}, nil
}
