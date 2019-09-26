package execution

import (
	"testing"

	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
	google_rpc_status "google.golang.org/genproto/googleapis/rpc/status"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/scheduler/scootapi/gen-go/scoot"
)

var rs *runStatus

func TestValidateExecResponse(t *testing.T) {
	req := &remoteexecution.ExecuteRequest{
		ActionDigest: &remoteexecution.Digest{
			Hash:      bazel.EmptySha,
			SizeBytes: bazel.EmptySize,
		},
	}
	err := validateExecRequest(req)
	if err != nil {
		t.Fatalf("Expected req to be ok")
	}

	req2 := &remoteexecution.ExecuteRequest{
		ActionDigest: &remoteexecution.Digest{
			Hash:      "1234",
			SizeBytes: bazel.EmptySize,
		},
	}
	err = validateExecRequest(req2)
	if err == nil {
		t.Fatalf("Expected req validation to fail")
	}

	req3 := &remoteexecution.ExecuteRequest{
		ActionDigest: &remoteexecution.Digest{
			Hash:      bazel.EmptySha,
			SizeBytes: int64(-2),
		},
	}
	err = validateExecRequest(req3)
	if err == nil {
		t.Fatalf("Expected req validation to fail")
	}
}

func TestValidateBzJobStatus(t *testing.T) {
	js := &scoot.JobStatus{}
	js.TaskData = make(map[string]*scoot.RunStatus)
	js.TaskData["task1"] = &scoot.RunStatus{Status: scoot.RunStatusState_COMPLETE}
	js.TaskData["task2"] = &scoot.RunStatus{Status: scoot.RunStatusState_UNKNOWN}
	err := validateBzJobStatus(js)
	if err == nil {
		t.Fatalf("Expected error, received %v", err)
	}

	js2 := &scoot.JobStatus{}
	js2.TaskStatus = make(map[string]scoot.Status)
	js2.TaskStatus["task1"] = scoot.Status_COMPLETED
	err = validateBzJobStatus(js2)
	if err != nil {
		t.Fatalf("Expected no error, received %v", err)
	}

	js3 := &scoot.JobStatus{}
	js3.TaskStatus = make(map[string]scoot.Status)
	err = validateBzJobStatus(js3)
	if err != nil {
		t.Fatalf("Expected no error, received %v", err)
	}
}

func TestRunStatusToExecuteOperationMetadata_Stage(t *testing.T) {
	oms := runStatusToExecuteOperationMetadata_Stage(rs)
	expected := remoteexecution.ExecuteOperationMetadata_UNKNOWN
	if oms != expected {
		t.Fatalf("Expected %v, got %v", expected, oms)
	}
}

func TestRunStatusToGoogleRpcStatus(t *testing.T) {
	s := runStatusToGoogleRpcStatus(rs)
	expected := &google_rpc_status.Status{}
	if s.Code != expected.Code ||
		s.Message != expected.Message ||
		s.Details != nil ||
		expected.Details != nil {
		t.Fatalf("Expected %#v, got %#v", expected, s)
	}
}

func TestRunStatusToDoneBool(t *testing.T) {
	done := runStatusToDoneBool(rs)
	if done != false {
		t.Fatalf("Expected false, got %v", done)
	}
}

func TestGetBazelResult(t *testing.T) {
	br := rs.GetBazelResult()
	if br != nil {
		t.Fatalf("Expected nil BazelResult, got %v", br)
	}
}
