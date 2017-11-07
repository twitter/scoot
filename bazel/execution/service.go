// Bazel Remote Execution API gRPC server
// Contains limited implementation of the Execution API interface
package execution

import (
	"fmt"
	"net"

	"github.com/golang/protobuf/ptypes"
	uuid "github.com/nu7hatch/gouuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	googlelongrunning "google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"

	"github.com/twitter/scoot/common/grpchelpers"
	scootproto "github.com/twitter/scoot/common/proto"
)

// Implements GRPCServer and remoteexecution.ExecutionServer interfaces
type executionServer struct {
	listener net.Listener
	server   *grpc.Server
}

// Creates a new GRPCServer (executionServer) based on a listener, and preregisters the service
func NewExecutionServer(l net.Listener) *executionServer {
	g := executionServer{listener: l, server: grpchelpers.NewServer()}
	remoteexecution.RegisterExecutionServer(g.server, &executionServer{})
	return &g
}

func (s *executionServer) Serve() error {
	log.Info("Serving GRPC Execution API on: ", s.listener.Addr())
	return s.server.Serve(s.listener)
}

// Execution APIs

// Stub of Execute API - most fields omitted, but returns a valid hardcoded response.
// Takes an ExecuteRequest and forms an ExecuteResponse that is returned as part of a
// google LongRunning Operation message.
func (s *executionServer) Execute(
	ctx context.Context,
	req *remoteexecution.ExecuteRequest) (*googlelongrunning.Operation, error) {
	// Get digest of request Action from wire format only, for inclusion in response metadata.
	actionSha, actionLen, err := scootproto.GetSha256(req.Action)
	if err != nil {
		return nil, err
	}

	op := googlelongrunning.Operation{}

	// Generate a UUID as a stub job identifier
	id, _ := uuid.NewV4()
	op.Name = fmt.Sprintf("operations/%s", id.String())
	op.Done = true

	eom := remoteexecution.ExecuteOperationMetadata{}
	eom.Stage = remoteexecution.ExecuteOperationMetadata_COMPLETED
	eom.ActionDigest = &remoteexecution.Digest{Hash: actionSha, SizeBytes: actionLen}

	// Marshal ExecuteActionMetadata to protobuf.Any format
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

	// Marshal ExecuteResponse to protobuf.Any format
	resAsPBAny, err := ptypes.MarshalAny(&res)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal ExecuteResponse as ptypes/any.Any: %v", err)
	}

	// Include the response message in the longrunning operation message
	op.Result = &googlelongrunning.Operation_Response{Response: resAsPBAny}
	return &op, nil
}
