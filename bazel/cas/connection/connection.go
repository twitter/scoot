package connection

/*
Interfaces needed to allow testing to mock out grpc connection behavior
*/
import (
	"google.golang.org/grpc"
)

type ClientConnPtr interface {
	Close() error
}

type GRPCDialer interface {
	Dial(target string, opts ...grpc.DialOption) (ClientConnPtr, error)
}

// the following are the CASClient's default connection components
type realGRPCDialer struct{}

func (rd *realGRPCDialer) Dial(target string, opts ...grpc.DialOption) (ClientConnPtr, error) {
	return grpc.Dial(target, opts...)
}

func MakeRealGRPCDialer() *realGRPCDialer {
	return &realGRPCDialer{}
}
