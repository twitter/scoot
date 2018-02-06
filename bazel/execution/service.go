// Bazel Remote Execution API gRPC server
// Contains limited implementation of the Execution API interface
package execution

import (
	"fmt"
	"net"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/execution/bazelapi"
	bazelthrift "github.com/twitter/scoot/bazel/execution/bazelapi/gen-go/bazel"
	"github.com/twitter/scoot/common/grpchelpers"
	scootproto "github.com/twitter/scoot/common/proto"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/sched"
	"github.com/twitter/scoot/sched/scheduler"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
	"github.com/twitter/scoot/scootapi/server/api"
)

// Implements GRPCServer and remoteexecution.ExecutionServer interfaces
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
	job, err := execReqToScoot(req, actionSha)
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

	op := longrunning.Operation{}
	op.Name = fmt.Sprintf("operations/%s", id)
	op.Done = true

	eom := &remoteexecution.ExecuteOperationMetadata{
		Stage:        remoteexecution.ExecuteOperationMetadata_COMPLETED,
		ActionDigest: &remoteexecution.Digest{Hash: actionSha, SizeBytes: actionLen},
	}

	// Marshal ExecuteActionMetadata to protobuf.Any format
	eomAsPBAny, err := marshalAny(eom)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	op.Metadata = eomAsPBAny

	// Marshal ExecuteResponse to protobuf.Any format
	res := &remoteexecution.ExecuteResponse{
		Result: &remoteexecution.ActionResult{
			ExitCode: 0,
		},
		CachedResult: false,
	}
	resAsPBAny, err := marshalAny(res)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	log.Info("ExecuteRequest completed successfully")
	// Include the response message in the longrunning operation message
	op.Result = &longrunning.Operation_Response{Response: resAsPBAny}
	return &op, nil
}

// Extract Scoot-related job fields from request to populate a JobDef, and pass through bazel request
func execReqToScoot(req *remoteexecution.ExecuteRequest, actionSha string) (result sched.JobDefinition, err error) {
	if err := validateExecRequest(req); err != nil {
		return result, err
	}

	// NOTE fixed to lowest priority in early stages of Bazel support
	// ExecuteRequests do not have priority values, but the Action portion
	// contains Platform Properties which can be used to specify arbitary server-side behavior.
	result.Priority = sched.P0
	result.Tasks = []sched.TaskDefinition{}

	d, err := time.ParseDuration(fmt.Sprintf("%dms", scootproto.GetMsFromDuration(req.GetAction().GetTimeout())))
	if err != nil {
		log.Errorf("Failed to parse Timeout from Action: %s", err)
		return result, err
	}

	// Populate TaskDef and Command. Note that Argv and EnvVars are set with placeholders for these requests,
	// per Bazel API this data must be made available by the client in the CAS before submitting this request.
	// To prevent increasing load and complexity in the Scheduler, this lookup is done at run time on the Worker
	// which is required to support CAS interactions.
	var task sched.TaskDefinition
	task.TaskID = fmt.Sprintf("Bazel_ExecuteRequest_%s_%d", actionSha, time.Now().Unix())
	task.Command.Argv = []string{"BZ_PLACEHOLDER"}
	task.Command.EnvVars = make(map[string]string)
	task.Command.Timeout = d
	task.Command.SnapshotID = bazel.SnapshotIDFromDigest(req.GetAction().GetInputRootDigest())
	task.Command.ExecuteRequest = &bazelapi.ExecuteRequest{Request: *req}

	result.Tasks = append(result.Tasks, task)
	return result, nil
}

func validateExecRequest(req *remoteexecution.ExecuteRequest) error {
	if req == nil {
		return fmt.Errorf("Unexpected nil execute request")
	}
	cmdDigest := req.GetAction().GetCommandDigest()
	inputDigest := req.GetAction().GetInputRootDigest()
	if !bazel.IsValidDigest(cmdDigest.GetHash(), cmdDigest.GetSizeBytes()) {
		return fmt.Errorf("Request action command digest is invalid")
	}
	if !bazel.IsValidDigest(inputDigest.GetHash(), inputDigest.GetSizeBytes()) {
		return fmt.Errorf("Request action input root digest is invalid")
	}
	return nil
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

	// TODO(rcouto): This skips over the stats handler increment in github.com/twitter/scoot/scootapi/server/server.go
	// Do we want to increment that counter? A different counter? Both?
	js, err := api.GetJobStatus(req.Name, s.sagaCoord)
	if err != nil {
		return nil, err
	}
	log.Info("Received job status %s", js)

	op := longrunning.Operation{}
	op.Name = fmt.Sprintf("operations/%s", js.ID)
	op.Done = true

	// There should only be one task per job, so runStatus/status is the
	// first & last one we encounter when looping through js.TaskData/TaskStatus
	if len(js.GetTaskData()) > 1 || len(js.GetTaskStatus()) > 1 {
		return nil, fmt.Errorf(
			"TaskData and/or TaskStatus of Bazel job status has len > 1. TaskData: %+v. TaskStatus: %+v",
			js.GetTaskData(), js.GetTaskStatus())
	}
	var runStatus *scoot.RunStatus
	for _, rs := range js.GetTaskData() {
		// map from taskID to runStatus
		runStatus = rs
	}

	if len(js.GetTaskStatus()) > 0 {
		var status scoot.Status
		for _, s := range js.TaskStatus {
			status = s
		}
		if status != js.Status {
			return nil, fmt.Errorf("Mismatch between task Status and job Status: %s vs %s", status, js.Status)
		}
	}

	var br *bazelthrift.ActionResult_
	if runStatus == nil {
		br = scoot.RunStatus_BazelResult__DEFAULT
	} else {
		br = runStatus.GetBazelResult_()
	}
	actionResult := bazelapi.MakeActionResultDomainFromThrift(br)

	eom := &remoteexecution.ExecuteOperationMetadata{
		Stage: bazelapi.RunStatusToExecuteOperationMetadata_Stage(runStatus),
	}
	// TODO(rcouto): Add relevant metadata to eom
	// https://godoc.org/google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test#ExecuteOperationMetadata

	// Marshal ExecuteActionMetadata to protobuf.Any format
	eomAsPBAny, err := marshalAny(eom)
	if err != nil {
		return nil, err
	}
	op.Metadata = eomAsPBAny

	// Marshal ExecuteResponse to protobuf.Any format
	res := &remoteexecution.ExecuteResponse{
		Result:       actionResult.GetResult(),
		CachedResult: false,
	}

	resAsPBAny, err := marshalAny(res)
	if err != nil {
		return nil, err
	}

	log.Info("GetOperationRequest completed successfully")
	// Include the response message in the longrunning operation message
	op.Result = &longrunning.Operation_Response{Response: resAsPBAny}
	return &op, nil
}

func marshalAny(pb proto.Message) (*any.Any, error) {
	pbAny, err := ptypes.MarshalAny(pb)
	if err != nil {
		s := fmt.Sprintf("Failed to marshal proto message %q as Any: %s", pb, err)
		log.Error(s)
		return nil, err
	}
	return pbAny, nil
}
