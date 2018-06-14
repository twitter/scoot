package bazelapi

import (
	"testing"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/any"
	"github.com/golang/protobuf/ptypes/timestamp"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
	google_rpc_code "google.golang.org/genproto/googleapis/rpc/code"
	google_rpc_errdetails "google.golang.org/genproto/googleapis/rpc/errdetails"
	google_rpc_status "google.golang.org/genproto/googleapis/rpc/status"

	scootproto "github.com/twitter/scoot/common/proto"
)

func TestDomainThriftDomainExecReq(t *testing.T) {
	er := &ExecuteRequest{
		Request: &remoteexecution.ExecuteRequest{
			InstanceName:    "test",
			SkipCacheLookup: true,
			Action: &remoteexecution.Action{
				CommandDigest:     &remoteexecution.Digest{Hash: "abc123", SizeBytes: 10},
				InputRootDigest:   &remoteexecution.Digest{Hash: "def456", SizeBytes: 20},
				OutputFiles:       []string{"output"},
				OutputDirectories: []string{"/data"},
				Platform: &remoteexecution.Platform{
					Properties: []*remoteexecution.Platform_Property{
						&remoteexecution.Platform_Property{Name: "propname", Value: "propvalue"},
					},
				},
				Timeout:    scootproto.GetDurationFromMs(60000),
				DoNotCache: true,
			},
		},
		ActionDigest: &remoteexecution.Digest{},
	}

	tr := MakeExecReqThriftFromDomain(er)
	if tr == nil {
		t.Fatal("Unexpected nil thrift result")
	}

	result := MakeExecReqDomainFromThrift(tr)
	if result == nil {
		t.Fatal("Unexpected nil domain result")
	}

	if result.Request.InstanceName != er.Request.InstanceName ||
		result.Request.SkipCacheLookup != er.Request.SkipCacheLookup ||
		result.Request.Action.CommandDigest.Hash != er.Request.Action.CommandDigest.Hash ||
		result.Request.Action.InputRootDigest.SizeBytes != er.Request.Action.InputRootDigest.SizeBytes ||
		result.Request.Action.OutputFiles[0] != er.Request.Action.OutputFiles[0] ||
		result.Request.Action.OutputDirectories[0] != er.Request.Action.OutputDirectories[0] ||
		result.Request.Action.Platform.Properties[0].Value != er.Request.Action.Platform.Properties[0].Value ||
		scootproto.GetMsFromDuration(result.Request.Action.Timeout) != scootproto.GetMsFromDuration(er.Request.Action.Timeout) ||
		result.Request.Action.DoNotCache != er.Request.Action.DoNotCache ||
		result.String() != er.String() {
		t.Fatalf("Unexpected output from result\ngot:      %v\nexpected: %v", result, er)
	}
}

func TestDomainThriftDomainActionRes(t *testing.T) {
	pcf := &google_rpc_errdetails.PreconditionFailure{
		Violations: []*google_rpc_errdetails.PreconditionFailure_Violation{
			&google_rpc_errdetails.PreconditionFailure_Violation{
				Type:    PreconditionMissing,
				Subject: "blobs/abc123/99",
			},
		},
	}
	pcfAsAny, err := ptypes.MarshalAny(pcf)
	if err != nil {
		t.Fatalf("Failed to serialize PreconditionFailure as Any: %s", err)
	}

	ar := &ActionResult{
		Result: &remoteexecution.ActionResult{
			StdoutDigest: &remoteexecution.Digest{Hash: "curry", SizeBytes: 30},
			StderrDigest: &remoteexecution.Digest{Hash: "carr", SizeBytes: 4},
			StdoutRaw:    []byte("durant"),
			StderrRaw:    []byte("lynch"),
			OutputFiles: []*remoteexecution.OutputFile{
				&remoteexecution.OutputFile{
					Digest:       &remoteexecution.Digest{Hash: "green", SizeBytes: 23},
					Path:         "crabtree",
					Content:      []byte("thompson"),
					IsExecutable: true,
				},
			},
			OutputDirectories: []*remoteexecution.OutputDirectory{
				&remoteexecution.OutputDirectory{
					TreeDigest: &remoteexecution.Digest{Hash: "cooper", SizeBytes: 89},
					Path:       "iguodala",
				},
			},
			ExitCode: 1,
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				Worker:                         "lebron23goat",
				QueuedTimestamp:                &timestamp.Timestamp{Seconds: 4},
				WorkerStartTimestamp:           &timestamp.Timestamp{Nanos: 0},
				WorkerCompletedTimestamp:       &timestamp.Timestamp{Seconds: 1, Nanos: 1},
				InputFetchStartTimestamp:       &timestamp.Timestamp{Seconds: 1, Nanos: 1},
				InputFetchCompletedTimestamp:   &timestamp.Timestamp{Seconds: 1, Nanos: 2},
				ExecutionStartTimestamp:        &timestamp.Timestamp{Seconds: 1, Nanos: 1},
				ExecutionCompletedTimestamp:    &timestamp.Timestamp{Seconds: 1, Nanos: 2},
				OutputUploadStartTimestamp:     &timestamp.Timestamp{Seconds: 1, Nanos: 1},
				OutputUploadCompletedTimestamp: &timestamp.Timestamp{Seconds: 1, Nanos: 2},
			},
		},
		ActionDigest: &remoteexecution.Digest{Hash: "bacon", SizeBytes: 420},
		GRPCStatus: &google_rpc_status.Status{
			Code:    int32(google_rpc_code.Code_OK),
			Message: "okay",
			Details: []*any.Any{pcfAsAny},
		},
	}

	tr := MakeActionResultThriftFromDomain(ar)
	if tr == nil {
		t.Fatal("Unexpected nil thrift result")
	}

	result := MakeActionResultDomainFromThrift(tr)
	if result == nil {
		t.Fatal("Unexpected nil domain result")
	}

	// ActionResult
	if result.Result.StdoutDigest.Hash != ar.Result.StdoutDigest.Hash ||
		result.Result.StdoutDigest.SizeBytes != ar.Result.StdoutDigest.SizeBytes ||
		result.Result.StderrDigest.Hash != ar.Result.StderrDigest.Hash ||
		result.Result.StderrDigest.SizeBytes != ar.Result.StderrDigest.SizeBytes ||
		string(result.Result.StdoutRaw) != string(ar.Result.StdoutRaw) ||
		string(result.Result.StderrRaw) != string(ar.Result.StderrRaw) ||
		result.Result.OutputFiles[0].Digest.Hash != ar.Result.OutputFiles[0].Digest.Hash ||
		result.Result.OutputFiles[0].Digest.SizeBytes != ar.Result.OutputFiles[0].Digest.SizeBytes ||
		result.Result.OutputFiles[0].Path != ar.Result.OutputFiles[0].Path ||
		string(result.Result.OutputFiles[0].Content) != string(ar.Result.OutputFiles[0].Content) ||
		result.Result.OutputFiles[0].IsExecutable != ar.Result.OutputFiles[0].IsExecutable ||
		result.Result.OutputDirectories[0].TreeDigest.Hash != ar.Result.OutputDirectories[0].TreeDigest.Hash ||
		result.Result.OutputDirectories[0].TreeDigest.SizeBytes != ar.Result.OutputDirectories[0].TreeDigest.SizeBytes ||
		result.Result.ExitCode != ar.Result.ExitCode ||
		result.String() != ar.String() {
		t.Fatalf("Unexpected output from result\ngot:      %v\nexpected: %v", result.Result, ar.Result)
	}

	// ExecutionMetadata
	if result.Result.ExecutionMetadata.Worker != ar.Result.ExecutionMetadata.Worker ||
		!tsEquals(result.Result.ExecutionMetadata.QueuedTimestamp, ar.Result.ExecutionMetadata.QueuedTimestamp) ||
		!tsEquals(result.Result.ExecutionMetadata.WorkerStartTimestamp, ar.Result.ExecutionMetadata.WorkerStartTimestamp) ||
		!tsEquals(result.Result.ExecutionMetadata.WorkerCompletedTimestamp, ar.Result.ExecutionMetadata.WorkerCompletedTimestamp) ||
		!tsEquals(result.Result.ExecutionMetadata.InputFetchStartTimestamp, ar.Result.ExecutionMetadata.InputFetchStartTimestamp) ||
		!tsEquals(result.Result.ExecutionMetadata.InputFetchCompletedTimestamp, ar.Result.ExecutionMetadata.InputFetchCompletedTimestamp) ||
		!tsEquals(result.Result.ExecutionMetadata.ExecutionStartTimestamp, ar.Result.ExecutionMetadata.ExecutionStartTimestamp) ||
		!tsEquals(result.Result.ExecutionMetadata.ExecutionCompletedTimestamp, ar.Result.ExecutionMetadata.ExecutionCompletedTimestamp) ||
		!tsEquals(result.Result.ExecutionMetadata.OutputUploadStartTimestamp, ar.Result.ExecutionMetadata.OutputUploadStartTimestamp) ||
		!tsEquals(result.Result.ExecutionMetadata.OutputUploadCompletedTimestamp, ar.Result.ExecutionMetadata.OutputUploadCompletedTimestamp) {
		t.Fatalf("Unexpected output from ExecutionMetadata\ngot:      %v\nexpected: %v",
			result.Result.ExecutionMetadata, ar.Result.ExecutionMetadata)
	}

	// ActionDigest
	if result.ActionDigest.Hash != ar.ActionDigest.Hash ||
		result.ActionDigest.SizeBytes != result.ActionDigest.SizeBytes {
		t.Fatalf("Unexpected output from action digest\ngot:      %v\nexpected: %v", result.ActionDigest, ar.ActionDigest)
	}

	// GRPCStatus
	if result.GRPCStatus.Code != ar.GRPCStatus.Code ||
		result.GRPCStatus.Message != ar.GRPCStatus.Message ||
		len(result.GRPCStatus.Details) != len(ar.GRPCStatus.Details) {
		t.Fatalf("Unexpected output from grpc status\ngot:      %v\nexpected: %v", result.GRPCStatus, ar.GRPCStatus)
	}
	resPcf := &google_rpc_errdetails.PreconditionFailure{}
	err = ptypes.UnmarshalAny(result.GRPCStatus.Details[0], resPcf)
	if err != nil {
		t.Fatalf("Failed to deserialize thrift google rpc status as PreconditionFailure: %s", err)
	}
	if len(resPcf.Violations) != len(pcf.Violations) ||
		resPcf.Violations[0].Type != pcf.Violations[0].Type ||
		resPcf.Violations[0].Subject != pcf.Violations[0].Subject {
		t.Fatalf("Unexpected output from grpc status precondition failure\ngot:      %v\nexpected: %v", resPcf, pcf)
	}
}

func tsEquals(a *timestamp.Timestamp, b *timestamp.Timestamp) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return (a.Seconds == b.Seconds) && (a.Nanos == b.Nanos)
}
