// Remote Execution API gRPC server
package server

import (
	"crypto/sha256"
	"fmt"
	"net"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	uuid "github.com/nu7hatch/gouuid"
	"golang.org/x/net/context"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	google_longrunning "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
)

// gRPC server interface encapsulating gRPC operations and execution server
type GRPCServer interface {
	Serve() error
}

// Implements GRPCServer and remoteexecution.ExecutionServer interfaces
type executionServer struct {
	listener net.Listener
	server   *grpc.Server
}

func NewExecutionServer(l net.Listener) *executionServer {
	g := executionServer{listener: l, server: grpc.NewServer()}
	remoteexecution.RegisterExecutionServer(g.server, &executionServer{})
	return &g
}

// Stub of Execute API - most fields omitted, but returns a valid hardcoded response
func (s *executionServer) Execute(ctx context.Context, req *remoteexecution.ExecuteRequest) (*google_longrunning.Operation, error) {
	// Get digest of request Action from wire format only
	// TODO convert this to library function
	actionBytes, err := proto.Marshal(req.Action)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal ExecuteRequest Action for digest: %v", err)
	}
	actionSha := fmt.Sprintf("%x", sha256.Sum256(actionBytes))

	op := google_longrunning.Operation{}

	id, _ := uuid.NewV4()
	op.Name = fmt.Sprintf("operations/%s", id.String())
	op.Done = true

	eom := remoteexecution.ExecuteOperationMetadata{}
	eom.Stage = remoteexecution.ExecuteOperationMetadata_COMPLETED
	eom.ActionDigest = &remoteexecution.Digest{Hash: actionSha, SizeBytes: int64(len(actionBytes))}

	// Marshal ExecuteActionMetadata to Any format
	eomAsPBAny, err := ptypes.MarshalAny(&eom)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal ExecuteOperationMetadata as ptypes/any.Any: %v", err)
	}
	op.Metadata = eomAsPBAny

	res := remoteexecution.ExecuteResponse{}
	ar := remoteexecution.ActionResult{}
	ar.ExitCode = 0
	res.Result = &ar
	res.CachedResult = false

	// Marshal ExecuteResponse to Any format
	resAsPBAny, err := ptypes.MarshalAny(&res)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal ExecuteResponse as ptypes/any.Any: %v", err)
	}

	op.Result = &google_longrunning.Operation_Response{Response: resAsPBAny}
	return &op, nil
}

func (s *executionServer) Serve() error {
	return s.server.Serve(s.listener)
}
