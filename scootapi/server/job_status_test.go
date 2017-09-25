package server

import (
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/twitter/scoot/common/thrifthelpers"
	"github.com/twitter/scoot/runner"
	s "github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/saga/sagalogs"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
	"github.com/twitter/scoot/workerapi"
	"github.com/twitter/scoot/workerapi/gen-go/worker"
)

func Test_GetJobStatus_InternalLogError(t *testing.T) {

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := s.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages("job1").Return(nil, s.NewInternalLogError("test error"))
	sagaCoord := s.MakeSagaCoordinator(sagaLogMock)

	status, err := GetJobStatus("job1", sagaCoord)
	if err == nil {
		t.Error("Expected error to be returned when SagaLog fails to retrieve messages")
	}

	switch err.(type) {
	case *scoot.ScootServerError:
	default:
		t.Error("Expected returned error to be ScootServerError", err)
	}

	if status.ID != "" || status.Status != scoot.Status_NOT_STARTED {
		t.Error("Expected Default JobStatus to be returned when error occurs")
	}
}

func Test_GetJobStatus_InvalidRequestError(t *testing.T) {

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := s.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages("job1").Return(nil, s.NewInvalidRequestError("test error"))
	sagaCoord := s.MakeSagaCoordinator(sagaLogMock)

	status, err := GetJobStatus("job1", sagaCoord)
	if err == nil {
		t.Error("Expected error to be returned when SagaLog fails to retrieve messages")
	}

	switch err.(type) {
	case *scoot.InvalidRequest:
	default:
		t.Error("Expected returned error to be ScootServerError", err)
	}

	if status.ID != "" || status.Status != scoot.Status_NOT_STARTED {
		t.Error("Expected Default JobStatus to be returned when error occurs")
	}
}

func Test_GetJobStatus_NoSagaMessages(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	sagaLogMock := s.NewMockSagaLog(mockCtrl)
	sagaLogMock.EXPECT().GetMessages("job1").Return(nil, nil)
	sagaCoord := s.MakeSagaCoordinator(sagaLogMock)

	status, err := GetJobStatus("job1", sagaCoord)
	if err != nil {
		t.Error("Unexpected error returned", err)
	}

	if status.ID != "job1" && status.Status != scoot.Status_IN_PROGRESS {
		t.Error("Unexpected JobStatus Returned")
	}
}

func Test_RunStatusThriftConversion(t *testing.T) {
	// test with non-empty structure
	var outURI = "outURI"
	var errURI = "errURI"
	var errorMsg = "error"
	var exitCode = int32(23)
	var workerRunStatus = &worker.RunStatus{Status: worker.Status_ABORTED, RunId: "runId", OutUri: &outURI, ErrUri: &errURI, Error: &errorMsg, ExitCode: &exitCode}
	var asBytes, _ = thrifthelpers.JsonSerialize(workerRunStatus)

	var scootRunStatus *scoot.RunStatus
	var err error
	if scootRunStatus, err = workerRunStatusToScootRunStatus(asBytes); err != nil {
		t.Errorf("converting non-nil filled worker.RunStatus to scoot.RunStatus returned an error:%s\n", err.Error())
	} else if runStatusEqual(scootRunStatus, workerRunStatus) {
		t.Errorf("converting non-nil filled worker.RunStatus to scoot.RunStatus did not match, expected %v go %v\n", workerRunStatus, scootRunStatus)
	}

	// test with empty structure
	workerRunStatus = &worker.RunStatus{}
	asBytes, _ = thrifthelpers.JsonSerialize(workerRunStatus)

	if scootRunStatus, err = workerRunStatusToScootRunStatus(asBytes); err != nil {
		t.Errorf("converting nil filled serialized worker.RunStatus to scoot.RunStatus returned an error:%s\n", err.Error())
	} else if runStatusEqual(scootRunStatus, workerRunStatus) {
		t.Errorf("converting nil filled worker.RunStatus to scoot.RunStatus did not match, expected %v go %v\n", workerRunStatus, scootRunStatus)
	}

	// test with nil structure
	asBytes, _ = thrifthelpers.JsonSerialize(nil)

	if scootRunStatus, err = workerRunStatusToScootRunStatus(asBytes); err != nil {
		t.Errorf("converting nil filled serialized worker.RunStatus to scoot.RunStatus returned an error:%s\n", err.Error())
	} else if scootRunStatus != nil {
		t.Errorf("converting nil worker.RunStatus to scoot.RunStatus did not match, expected nil go %v\n", scootRunStatus)
	}

}

func runStatusEqual(scootRunStatus *scoot.RunStatus, workerRunStatus *worker.RunStatus) bool {
	return scootRunStatus.RunId != workerRunStatus.RunId ||
		!strPtrCompare(scootRunStatus.Error, workerRunStatus.Error) ||
		!strPtrCompare(scootRunStatus.ErrUri, workerRunStatus.ErrUri) ||
		!int32PtrCompare(scootRunStatus.ExitCode, workerRunStatus.ExitCode) ||
		!strPtrCompare(scootRunStatus.OutUri, workerRunStatus.OutUri) ||
		scootRunStatus.Status.String() != workerRunStatus.Status.String()
}

// TODO should this be in a utility somewhere (does one already exist)?
func strPtrCompare(s1 *string, s2 *string) bool {

	if s1 == nil && s2 == nil {
		return true
	}
	if s1 == nil || s2 == nil {
		return false
	}
	return strings.Compare(*s1, *s2) == 0
}

// TODO should this be in a utility somewhere (does one already exist)?
func int32PtrCompare(i1 *int32, i2 *int32) bool {
	if i1 == nil && i2 == nil {
		return true
	}
	if i1 == nil || i2 == nil {
		return false
	}
	return *i1 == *i2
}

func validateRunResult(resultsAsByte []byte, taskId string) bool {
	runResults := scoot.RunStatus{}
	thrifthelpers.JsonDeserialize(&runResults, resultsAsByte)

	if runResults.RunId != taskId {
		log.Infof("Run ids didn't match. got: %s,  expected: %s\n", taskId, runResults.RunId)
		return false
	}
	if runResults.Status < scoot.RunStatusState_COMPLETE {
		log.Infof("Taskid: %s, Invalid run status: %v\n", taskId, runResults.Status)
		return false
	}
	if int(*runResults.ExitCode) != 0 && int(*runResults.ExitCode) != -1 {
		log.Infof("Taskid: %s, Invalid exit code: %d\n", taskId, runResults.ExitCode)
		return false
	}
	if !strings.Contains(*runResults.Error, "error ") {
		log.Infof("Taskid: %s, Invalid error string: %s\n", taskId, *runResults.Error)
		return false
	}
	if !strings.Contains(*runResults.OutUri, "out URI ") {
		log.Infof("Taskid: %s, Invalid out URI: %s\n", taskId, *runResults.OutUri)
		return false
	}
	if !strings.Contains(*runResults.ErrUri, "error URI ") {
		log.Infof("Taskid: %s, Invalid err URI: %s\n", taskId, *runResults.ErrUri)
		return false
	}

	return true
}

func TestRunStatusRoundTrip(t *testing.T) {
	sagaLog := sagalogs.MakeInMemorySagaLog()
	sagaCoord := s.MakeSagaCoordinator(sagaLog)

	jobID := "foo"
	saga, err := sagaCoord.MakeSaga(jobID, nil)
	if err != nil {
		t.Fatal(err)
	}

	taskID := "t"
	if err = saga.StartTask(taskID, nil); err != nil {
		t.Fatal(err)
	}

	stdoutRef := "http://example.com/stdout"

	st := runner.RunStatus{
		RunID:     runner.RunID("2"),
		State:     runner.COMPLETE,
		StdoutRef: stdoutRef,
		StderrRef: "",
		ExitCode:  0,
		Error:     "",
	}
	statusAsBytes, err := workerapi.SerializeProcessStatus(st)
	if err != nil {
		t.Fatal(err)
	}

	if err = saga.EndTask(taskID, statusAsBytes); err != nil {
		t.Fatal(err)
	}

	jobStatus, err := GetJobStatus(jobID, sagaCoord)
	if err != nil {
		t.Fatal(err)
	}

	runStatus := jobStatus.TaskData[taskID]
	if *runStatus.OutUri != stdoutRef {
		t.Fatalf("runStatus.OutUri: %v (expected %v)", *runStatus.OutUri, stdoutRef)
	}
}
