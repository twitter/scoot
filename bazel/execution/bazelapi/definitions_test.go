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
)

func TestDomainThriftDomainExecReq(t *testing.T) {
	er := &ExecuteRequest{
		Request: &remoteexecution.ExecuteRequest{
			InstanceName:       "test",
			SkipCacheLookup:    true,
			ActionDigest:       &remoteexecution.Digest{},
			ExecutionPolicy:    &remoteexecution.ExecutionPolicy{Priority: 1},
			ResultsCachePolicy: &remoteexecution.ResultsCachePolicy{Priority: 0},
		},
		ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
			QueuedTimestamp: &timestamp.Timestamp{Nanos: 25},
		},
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
		!digestEquals(result.Request.ActionDigest, er.Request.ActionDigest) ||
		result.Request.ExecutionPolicy.Priority != er.Request.ExecutionPolicy.Priority ||
		result.Request.ResultsCachePolicy.Priority != er.Request.ResultsCachePolicy.Priority ||
		result.ExecutionMetadata.QueuedTimestamp.Nanos != er.ExecutionMetadata.QueuedTimestamp.Nanos {
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
	if !digestEquals(result.Result.StdoutDigest, ar.Result.StdoutDigest) ||
		!digestEquals(result.Result.StderrDigest, ar.Result.StderrDigest) ||
		string(result.Result.StdoutRaw) != string(ar.Result.StdoutRaw) ||
		string(result.Result.StderrRaw) != string(ar.Result.StderrRaw) ||
		!digestEquals(result.Result.OutputFiles[0].Digest, ar.Result.OutputFiles[0].Digest) ||
		result.Result.OutputFiles[0].Path != ar.Result.OutputFiles[0].Path ||
		result.Result.OutputFiles[0].IsExecutable != ar.Result.OutputFiles[0].IsExecutable ||
		!digestEquals(result.Result.OutputDirectories[0].TreeDigest, ar.Result.OutputDirectories[0].TreeDigest) ||
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
	if !digestEquals(result.ActionDigest, ar.ActionDigest) {
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

func digestEquals(a, b *remoteexecution.Digest) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return (a.SizeBytes == b.SizeBytes) && (a.Hash == b.Hash)
}

func tsEquals(a, b *timestamp.Timestamp) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return (a.Seconds == b.Seconds) && (a.Nanos == b.Nanos)
}
