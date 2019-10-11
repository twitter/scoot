package conn_interfaces

import (
	"google.golang.org/grpc"
)

type ClientConnPtr interface {
	Close() error
}

type GRPCDialer interface {
	Dial(target string, opts ...grpc.DialOption) (ClientConnPtr, error)
}
