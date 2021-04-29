// Bazel Remote Execution API gRPC server
// Contains limited implementation of the Execution API interface
package execution

import (
	"fmt"
	"net"

	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/longrunning"
	google_rpc_code "google.golang.org/genproto/googleapis/rpc/code"
	google_rpc_status "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/execution/bazelapi"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
	loghelpers "github.com/twitter/scoot/common/log/helpers"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/scheduler/api/thrift"
	"github.com/twitter/scoot/scheduler/domain"
	"github.com/twitter/scoot/scheduler/server"
)

// Implements GRPCServer, remoteexecution.ExecutionServer, and longrunning.OperationsServer interfaces
type executionServer struct {
	listener  net.Listener
	sagaCoord saga.SagaCoordinator
	server    *grpc.Server
	scheduler server.Scheduler
	stat      stats.StatsReceiver
}

// Creates a new GRPCServer (executionServer) based on a GRPC config, scheduler, and stats, and preregisters the service
func MakeExecutionServer(gc *bazel.GRPCConfig, s server.Scheduler, stat stats.StatsReceiver) *executionServer {
	if gc == nil {
		return nil
	}

	l, err := gc.NewListener()
	if err != nil {
		panic(err)
	}
	gs := gc.NewGRPCServer()
	g := executionServer{
		listener:  l,
		sagaCoord: s.GetSagaCoord(),
		server:    gs,
		scheduler: s,
		stat:      stat,
	}
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

// Takes an ExecuteRequest and forms an ExecuteResponse that is returned as part of a
// google LongRunning Operation message via a stream.
// By convention, we will not reuse this stream and the client should
// continue to use GetOperation polling to determine execution state.
func (s *executionServer) Execute(
	req *remoteexecution.ExecuteRequest, execServer remoteexecution.Execution_ExecuteServer) error {
	log.Debugf("Received Execute request: %s", req)

	if !s.IsInitialized() {
		return status.Error(codes.Internal, "Server not initialized")
	}

	var err error = nil

	// Record metrics based on final error condition
	defer func() {
		if err == nil {
			s.stat.Counter(stats.BzExecSuccessCounter).Inc(1)
		} else {
			s.stat.Counter(stats.BzExecFailureCounter).Inc(1)
		}
	}()
	defer s.stat.Latency(stats.BzExecLatency_ms).Time().Stop()

	// Transform ExecuteRequest into Scoot Job, validate and schedule
	// If we encounter an error here, assume it was due to an InvalidArgument
	job, err := execReqToScoot(req)
	if err != nil {
		log.Errorf("Failed to convert request to Scoot JobDefinition: %s", err)
		return status.Error(codes.InvalidArgument, fmt.Sprintf("Error converting request to internal definition: %s", err))
	}

	err = domain.ValidateJob(job)
	if err != nil {
		log.Errorf("Scoot Job generated from request invalid: %s", err)
		return status.Error(codes.Internal, fmt.Sprintf("Internal job definition invalid: %s", err))
	}

	id, err := s.scheduler.ScheduleJob(job)
	if err != nil {
		log.Errorf("Failed to schedule Scoot job: %s", err)
		return status.Error(codes.Internal, fmt.Sprintf("Failed to schedule Scoot job: %s", err))
	}
	log.WithFields(
		log.Fields{
			"jobID": id,
		}).Info("Scheduled execute request as Scoot job")

	eom := &remoteexecution.ExecuteOperationMetadata{
		Stage:        remoteexecution.ExecuteOperationMetadata_QUEUED,
		ActionDigest: req.GetActionDigest(),
	}

	// Marshal ExecuteActionMetadata to protobuf.Any format
	eomAsPBAny, err := marshalAny(eom)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	// Include the response message in the longrunning operation message
	op := &longrunning.Operation{
		Name:     id,
		Metadata: eomAsPBAny,
		Done:     false,
	}

	// Send the initial operation on the exec server stream
	err = execServer.Send(op)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	log.Debug("ExecuteRequest completed successfully")
	return nil
}

func (s *executionServer) WaitExecution(
	*remoteexecution.WaitExecutionRequest,
	remoteexecution.Execution_WaitExecutionServer) error {
	return status.Error(codes.Unimplemented, fmt.Sprint("Unsupported in Scoot"))
}

// Google LongRunning APIs

// Takes a GetOperation request and forms an ExecuteResponse that is returned as part of a
// google LongRunning Operation message.
// Note that the ActionDigest field in the ExecuteOperationMetadata is not always available for
// tasks that have not completed.
func (s *executionServer) GetOperation(
	_ context.Context,
	req *longrunning.GetOperationRequest) (*longrunning.Operation, error) {
	log.Debugf("Received GetOperation request: %v", req)

	if !s.IsInitialized() {
		return nil, status.Error(codes.Internal, "Server not initialized")
	}

	var err error = nil

	// Record metrics based on final error condition
	defer func() {
		if err == nil {
			s.stat.Counter(stats.BzGetOpSuccessCounter).Inc(1)
		} else {
			s.stat.Counter(stats.BzGetOpFailureCounter).Inc(1)
		}
	}()
	defer s.stat.Latency(stats.BzGetOpLatency_ms).Time().Stop()

	rs, err := s.getRunStatusAndValidate(req.GetName())
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

	isDone := runStatusToDoneBool(rs)
	op := longrunning.Operation{
		Name:     req.GetName(),
		Metadata: eomAsPBAny,
		Done:     runStatusToDoneBool(rs),
	}

	// If done, create ExecuteResponse in protobuf.Any format and include in Operation.Result.
	// If the run status' bazelapi.ActionResult contains a google rpc Status, return that
	// in the Response, otherwise convert the run status to a google rpc Status.
	if isDone {
		var grpcs *google_rpc_status.Status
		if actionResult != nil && actionResult.GRPCStatus != nil {
			grpcs = actionResult.GetGRPCStatus()
		} else {
			grpcs = runStatusToGoogleRpcStatus(rs)
		}
		if grpcs.GetCode() == int32(google_rpc_code.Code_CANCELLED) {
			op.Result = &longrunning.Operation_Error{
				Error: &google_rpc_status.Status{
					Code:    grpcs.GetCode(),
					Message: "CANCELLED",
				},
			}
		} else {
			res := &remoteexecution.ExecuteResponse{
				Result:       actionResult.GetResult(),
				CachedResult: actionResult.GetCached(),
				Status:       grpcs,
			}
			resAsPBAny, err := marshalAny(res)
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			op.Result = &longrunning.Operation_Response{
				Response: resAsPBAny,
			}
		}
	}
	log.Debug("GetOperationRequest completed successfully")
	return &op, nil
}

// Unsupported
func (s *executionServer) ListOperations(context.Context, *longrunning.ListOperationsRequest) (*longrunning.ListOperationsResponse, error) {
	return nil, status.Error(codes.Unimplemented, fmt.Sprint("Unsupported in Scoot"))
}

// Unsupported
func (s *executionServer) DeleteOperation(context.Context, *longrunning.DeleteOperationRequest) (*empty.Empty, error) {
	return nil, status.Error(codes.Unimplemented, fmt.Sprint("Unsupported in Scoot"))
}

// Unsupported
func (s *executionServer) WaitOperation(context.Context, *longrunning.WaitOperationRequest) (*longrunning.Operation, error) {
	return nil, status.Error(codes.Unimplemented, fmt.Sprint("Unsupported in Scoot"))
}

// Takes a CancelOperation request and asynchronously starts cancellation on the specified Operation
// via Scoot's KillJob API. Note that successful cancellation is not guaranteed. The client should use
// GetOperation to determine if the cancellation succeeded
func (s *executionServer) CancelOperation(_ context.Context, req *longrunning.CancelOperationRequest) (*empty.Empty, error) {
	log.Debugf("Received CancelOperation request: %v", req)
	go func() {
		var err error = nil
		// Record metrics based on final error condition
		defer func() {
			if err == nil {
				s.stat.Counter(stats.BzCancelOpSuccessCounter).Inc(1)
			} else {
				s.stat.Counter(stats.BzCancelOpFailureCounter).Inc(1)
			}
		}()
		defer s.stat.Latency(stats.BzCancelOpLatency_ms).Time().Stop()
		err = s.killJobAndValidate(req.GetName())
	}()

	return &empty.Empty{}, nil
}

// Internal functions

func (s *executionServer) getRunStatusAndValidate(jobID string) (*runStatus, error) {
	js, err := thrift.GetJobStatus(jobID, s.sagaCoord)
	if err != nil {
		return nil, err
	}
	log.Debugf("Received job status %s", js)

	err = validateBzJobStatus(js)
	if err != nil {
		return nil, err
	}
	loghelpers.LogRunStatus(js)

	var rs runStatus
	for _, rStatus := range js.GetTaskData() {
		rs = runStatus{rStatus}
	}
	return &rs, nil
}

func (s *executionServer) killJobAndValidate(jobID string) error {
	js, err := thrift.KillJob(jobID, s.scheduler, s.sagaCoord)
	if err != nil {
		return err
	}
	log.Debugf("Received job status after kill request %s", js)

	err = validateBzJobStatus(js)
	if err != nil {
		return err
	}
	loghelpers.LogRunStatus(js)

	return nil
}
