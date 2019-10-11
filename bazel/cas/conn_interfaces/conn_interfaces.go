package conn_interfaces
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
