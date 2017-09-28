// This is where remote grpc server API will live.
// This will get used/imported by scheduler via ice.
// TODO - change protocol to pb per conventions?
package server

import (
	"context"

	"github.com/twitter/scoot/remote/protocol"
)

// Implements protocol ExecutionServer interface
func Execute(context.Context, *protocol.RequestPlaceholder) (*protocol.ResponsePlaceholder, error) {
	rp := protocol.ResponsePlaceholder{Output: "execute!!!"}
	return &rp, nil
}
