// Bazel Remote Execution API gRPC server
// Contains limited implementation of the Execution API interface
package execution

import (
	"fmt"
	"net"

	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twitter/scoot/bazel/execution/bazelapi"
	"github.com/twitter/scoot/common/grpchelpers"
	scootproto "github.com/twitter/scoot/common/proto"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/sched"
	"github.com/twitter/scoot/sched/scheduler"
	"github.com/twitter/scoot/scootapi/server/api"
)

// Implements GRPCServer, remoteexecution.ExecutionServer, and longrunning.OperationsServer interfaces
type executionServer struct {
	listener  net.Listener
	sagaCoord saga.SagaCoordinator
	server    *grpc.Server
	scheduler scheduler.Scheduler
}

// Creates a new GRPCServer (executionServer) based on a listener, and preregisters the service
func MakeExecutionServer(l net.Listener, s scheduler.Scheduler) *executionServer {
	g := executionServer{listener: l, server: grpchelpers.NewServer(), scheduler: s}
	remoteexecution.RegisterExecutionServer(g.server, &g)
	longrunning.RegisterOperationsServer(g.server, &g)
	return &g
}

func (s *executionServer) IsInitialized() bool {
	if s == nil {
		return false
	} else if s.scheduler == nil {
		return false
	}
	return true
}

func (s *executionServer) Serve() error {
	log.Infof("Serving GRPC Execution API on: %s", s.listener.Addr())
	return s.server.Serve(s.listener)
}

// Execution APIs

// Stub of Execute API - most fields omitted, but returns a valid hardcoded response.
// Takes an ExecuteRequest and forms an ExecuteResponse that is returned as part of a
// google LongRunning Operation message.
func (s *executionServer) Execute(
	_ context.Context,
	req *remoteexecution.ExecuteRequest) (*longrunning.Operation, error) {
	log.Infof("Received Execute request: %s", req)

	if !s.IsInitialized() {
		return nil, status.Error(codes.Internal, "Server not initialized")
	}

	// Get digest of request Action from wire format only, for inclusion in response metadata.
	actionSha, actionLen, err := scootproto.GetSha256(req.GetAction())
	if err != nil {
		log.Errorf("Failed to get digest of request action: %s", err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error serializing action: %s", err))
	}

	// Transform ExecuteRequest into Scoot Job, validate and schedule
	job, err := execReqToScoot(req, actionSha, actionLen)
	if err != nil {
		log.Errorf("Failed to convert request to Scoot JobDefinition: %s", err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error converting request to internal definition: %s", err))
	}

	err = sched.ValidateJob(job)
	if err != nil {
		log.Errorf("Scoot Job generated from request invalid: %s", err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("Internal job definition invalid: %s", err))
	}

	id, err := s.scheduler.ScheduleJob(job)
	if err != nil {
		log.Errorf("Failed to schedule Scoot job: %s", err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("Failed to schedule Scoot job: %s", err))
	}
	log.WithFields(
		log.Fields{
			"jobID": id,
		}).Info("Scheduled execute request as Scoot job")

	eom := &remoteexecution.ExecuteOperationMetadata{
		Stage: remoteexecution.ExecuteOperationMetadata_QUEUED,
		ActionDigest: &remoteexecution.Digest{
			Hash:      actionSha,
			SizeBytes: actionLen,
		},
	}

	// Marshal ExecuteActionMetadata to protobuf.Any format
	eomAsPBAny, err := marshalAny(eom)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Marshal ExecuteResponse to protobuf.Any format
	res := &remoteexecution.ExecuteResponse{}
	resAsPBAny, err := marshalAny(res)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	log.Info("ExecuteRequest completed successfully")
	// Include the response message in the longrunning operation message
	op := longrunning.Operation{
		Name:     fmt.Sprintf("operations/%s", id),
		Metadata: eomAsPBAny,
		Done:     false,
		Result: &longrunning.Operation_Response{
			Response: resAsPBAny,
		},
	}
	return &op, nil
}

// Takes a GetOperation request and forms an ExecuteResponse that is returned as part of a
// google LongRunning Operation message
func (s *executionServer) GetOperation(
	_ context.Context,
	req *longrunning.GetOperationRequest) (*longrunning.Operation, error) {
	log.Infof("Received GetOperation request: %s", req)

	if !s.IsInitialized() {
		return nil, status.Error(codes.Internal, "Server not initialized")
	}

	rs, err := s.getRunStatusAndValidate(req.Name)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	actionResult := bazelapi.MakeActionResultDomainFromThrift(rs.GetBazelResult())

	eom := &remoteexecution.ExecuteOperationMetadata{
		Stage:        runStatusToExecuteOperationMetadata_Stage(rs),
		ActionDigest: actionResult.GetActionDigest(),
	}

	// Marshal ExecuteActionMetadata to protobuf.Any format
	eomAsPBAny, err := marshalAny(eom)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Marshal ExecuteResponse to protobuf.Any format
	res := &remoteexecution.ExecuteResponse{
		Result:       actionResult.GetResult(),
		CachedResult: false,
		Status:       runStatusToGoogleRpcStatus(rs),
	}
	resAsPBAny, err := marshalAny(res)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	log.Info("GetOperationRequest completed successfully")
	// Include the response message in the longrunning operation message
	op := longrunning.Operation{
		Name:     fmt.Sprintf("operations/%s", req.Name),
		Metadata: eomAsPBAny,
		Done:     runStatusToDoneBool(rs),
		Result: &longrunning.Operation_Response{
			Response: resAsPBAny,
		},
	}
	return &op, nil
}

func (s *executionServer) ListOperations(context.Context, *longrunning.ListOperationsRequest) (*longrunning.ListOperationsResponse, error) {
	return nil, status.Error(codes.Internal, fmt.Sprint("Not implemented"))
}

func (s *executionServer) DeleteOperation(context.Context, *longrunning.DeleteOperationRequest) (*empty.Empty, error) {
	return nil, status.Error(codes.Internal, fmt.Sprint("Not implemented"))
}

func (s *executionServer) CancelOperation(context.Context, *longrunning.CancelOperationRequest) (*empty.Empty, error) {
	return nil, status.Error(codes.Internal, fmt.Sprint("Not implemented"))
}

func (s *executionServer) getRunStatusAndValidate(jobID string) (*runStatus, error) {
	js, err := api.GetJobStatus(jobID, s.sagaCoord)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error getting job status: %s", err))
	}
	log.Info("Received job status %s", js)

	err = validateBzJobStatus(js)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var rs runStatus
	for _, rStatus := range js.GetTaskData() {
		rs = runStatus{rStatus}
	}
	return &rs, nil
}
