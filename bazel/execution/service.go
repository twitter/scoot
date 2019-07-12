// Bazel Remote Execution API gRPC server
// Contains limited implementation of the Execution API interface
package execution

import (
	"fmt"
	"net"

	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/longrunning"
	google_rpc_status "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/execution/bazelapi"
	loghelpers "github.com/twitter/scoot/common/log/helpers"
	"github.com/twitter/scoot/common/stats"
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
	stat      stats.StatsReceiver
}

// Creates a new GRPCServer (executionServer) based on a GRPC config, scheduler, and stats, and preregisters the service
func MakeExecutionServer(gc *bazel.GRPCConfig, s scheduler.Scheduler, stat stats.StatsReceiver) *executionServer {
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

	err = sched.ValidateJob(job)
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

	isDone := runStatusToDoneBool(rs)
	op := longrunning.Operation{
		Name:     req.Name,
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

	log.Debug("GetOperationRequest completed successfully")
	return &op, nil
}

func (s *executionServer) ListOperations(context.Context, *longrunning.ListOperationsRequest) (*longrunning.ListOperationsResponse, error) {
	return nil, status.Error(codes.Unimplemented, fmt.Sprint("Unsupported in Scoot"))
}

// TODO hook up to Job Kill API
func (s *executionServer) DeleteOperation(context.Context, *longrunning.DeleteOperationRequest) (*empty.Empty, error) {
	return nil, status.Error(codes.Unimplemented, fmt.Sprint("Unsupported in Scoot"))
}

func (s *executionServer) CancelOperation(context.Context, *longrunning.CancelOperationRequest) (*empty.Empty, error) {
	return nil, status.Error(codes.Unimplemented, fmt.Sprint("Unsupported in Scoot"))
}

// Internal functions

func (s *executionServer) getRunStatusAndValidate(jobID string) (*runStatus, error) {
	js, err := api.GetJobStatus(jobID, s.sagaCoord)
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
