package grpchelpers

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// NewServer returns a new grpc server just like grpc.NewServer(), but
// which automatically implements the grpc server reflection protocol.
// See https://github.com/grpc/grpc/blob/master/doc/server-reflection.md
func NewServer(opt ...grpc.ServerOption) *grpc.Server {
	s := grpc.NewServer(opt...)
	reflection.Register(s)
	return s
}
