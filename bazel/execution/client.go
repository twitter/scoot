package execution

import (
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/longrunning"
	google_rpc_code "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/common/proto"
)

// Google Longrunning client APIs

// Make a GetOperation request against a server supporting the google.longrunning.operations API
// Takes a Resolver and name of the Operation to Get
func GetOperation(r dialer.Resolver, name string) (*longrunning.Operation, error) {
	serverAddr, err := r.Resolve()
	if err != nil {
		return nil, fmt.Errorf("Failed to resolve server address: %s", err)
	}

	cc, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Failed to dial server %s: %s", serverAddr, err)
	}
	defer cc.Close()

	req := &longrunning.GetOperationRequest{Name: name}

	opc := longrunning.NewOperationsClient(cc)
	return getFromClient(opc, req)
}

func getFromClient(opc longrunning.OperationsClient, req *longrunning.GetOperationRequest) (*longrunning.Operation, error) {
	return opc.GetOperation(context.Background(), req)
}

// Google Bazel Execution client APIs

// Makes an Execute request against a server supporting the google.devtools.remoteexecution API
// Takes a Resolver and ExecuteRequest data. Currently supported:
// * ActionDigest
// * SkipCache
// TBD support for:
// * Misc specs: InstanceName, Policies
func Execute(r dialer.Resolver, actionDigest *remoteexecution.Digest, skipCache bool) (*longrunning.Operation, error) {
	serverAddr, err := r.Resolve()
	if err != nil {
		return nil, fmt.Errorf("Failed to resolve server address: %s", err)
	}

	cc, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Failed to dial server %s: %s", serverAddr, err)
	}
	defer cc.Close()

	req := &remoteexecution.ExecuteRequest{
		ActionDigest:    actionDigest,
		SkipCacheLookup: skipCache,
	}

	ec := remoteexecution.NewExecutionClient(cc)
	return execFromClient(ec, req)
}

// By convention, execute client recieves first Operation from stream and closes
func execFromClient(ec remoteexecution.ExecutionClient, req *remoteexecution.ExecuteRequest) (*longrunning.Operation, error) {
	execClient, err := ec.Execute(context.Background(), req)
	if err != nil {
		return nil, err
	}
	defer execClient.CloseSend()

	op, err := execClient.Recv()
	if err != nil {
		return nil, err
	}

	return op, nil
}

// Internal, util client functions

// Parse a generic longrunning.Operation structure into expected Execution API components.
// Deserializes Operation.Metadata as an ExecuteOperationMetadata always.
// Checks for Operation.Result as a Response and if found deserializes as an ExecuteResponse.
// Per Bazel API, does not check for Operation.Result as a GRPC Status/Error - not allowed.
func ParseExecuteOperation(op *longrunning.Operation) (*remoteexecution.ExecuteOperationMetadata,
	*remoteexecution.ExecuteResponse, error) {
	if op == nil {
		return nil, nil, fmt.Errorf("Attempting to parse nil Operation")
	}

	eom := &remoteexecution.ExecuteOperationMetadata{}
	err := ptypes.UnmarshalAny(op.GetMetadata(), eom)
	if err != nil {
		return nil, nil, fmt.Errorf("Error deserializing metadata as ExecuteOperationMetadata: %s", err)
	}

	if op.GetResponse() == nil {
		return eom, nil, nil
	}

	res := &remoteexecution.ExecuteResponse{}
	err = ptypes.UnmarshalAny(op.GetResponse(), res)
	if err != nil {
		return nil, nil, fmt.Errorf("Error deserializing response as ExecuteResponse: %s", err)
	}
	return eom, res, nil
}

// String conversion for human consumption of Operation's nested data
func ExecuteOperationToStr(op *longrunning.Operation) string {
	eom, res, err := ParseExecuteOperation(op)
	if err != nil {
		return ""
	}
	s := fmt.Sprintf("Operation: %s\n\tDone: %t\n", op.GetName(), op.GetDone())
	s += fmt.Sprintf("\tMetadata:\n")
	s += fmt.Sprintf("\t\tStage: %s\n", eom.GetStage())
	s += fmt.Sprintf("\t\tActionDigest: %s\n", bazel.DigestToStr(eom.GetActionDigest()))
	if res != nil {
		s += fmt.Sprintf("\tExecResponse:\n")
		s += fmt.Sprintf("\t\tStatus: %s\n", res.GetStatus())
		s += fmt.Sprintf("\t\t\tCode: %s\n", google_rpc_code.Code_name[res.GetStatus().GetCode()])
		s += fmt.Sprintf("\t\tCached: %t\n", res.GetCachedResult())
		s += fmt.Sprintf("\t\tActionResult:\n")
		s += fmt.Sprintf("\t\t\tExitCode: %d\n", res.GetResult().GetExitCode())
		s += fmt.Sprintf("\t\t\tOutputFiles: %s\n", res.GetResult().GetOutputFiles())
		s += fmt.Sprintf("\t\t\tOutputDirectories: %s\n", res.GetResult().GetOutputDirectories())
		s += fmt.Sprintf("\t\t\tStdoutDigest: %s\n", bazel.DigestToStr(res.GetResult().GetStdoutDigest()))
		s += fmt.Sprintf("\t\t\tStderrDigest: %s\n", bazel.DigestToStr(res.GetResult().GetStderrDigest()))
		if res.GetResult().GetExecutionMetadata() != nil {
			em := res.GetResult().GetExecutionMetadata()
			s += fmt.Sprintf("\t\t\tExecutionMetadata:\n")
			s += fmt.Sprintf("\t\t\t\tWorker: %s\n", em.GetWorker())
			s = addLatencyToStr(s, "\t\t\t\t", "QueueLatency", em.GetQueuedTimestamp(), em.GetWorkerStartTimestamp())
			s = addLatencyToStr(s, "\t\t\t\t", "WorkerTotal", em.GetWorkerStartTimestamp(), em.GetWorkerCompletedTimestamp())
			s = addLatencyToStr(s, "\t\t\t\t", "InputFetch", em.GetInputFetchStartTimestamp(), em.GetInputFetchCompletedTimestamp())
			s = addLatencyToStr(s, "\t\t\t\t", "Execution", em.GetExecutionStartTimestamp(), em.GetExecutionCompletedTimestamp())
			s = addLatencyToStr(s, "\t\t\t\t", "OutputUpload", em.GetOutputUploadStartTimestamp(), em.GetOutputUploadCompletedTimestamp())
		}
	}
	return s
}

func addLatencyToStr(inputStr, indent, label string, startTs, endTs *timestamp.Timestamp) string {
	if startTs == nil || endTs == nil {
		return inputStr
	}
	startTime, endTime := proto.GetTimeFromTimestamp(startTs), proto.GetTimeFromTimestamp(endTs)
	duration := endTime.Sub(startTime)
	ms := duration.Nanoseconds() / int64(time.Millisecond)
	inputStr += fmt.Sprintf("%s%s: %dms\n", indent, label, ms)
	return inputStr
}
