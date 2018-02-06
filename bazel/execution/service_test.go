package execution

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes"
	"golang.org/x/net/context"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/genproto/googleapis/longrunning"

	scootproto "github.com/twitter/scoot/common/proto"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/sched/scheduler"
)

// Determine that Execute can accept a well-formed request and returns a well-formed response
func TestExecuteStub(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sc := scheduler.NewMockScheduler(mockCtrl)
	sc.EXPECT().ScheduleJob(gomock.Any()).Return("testJobID", nil)

	s := executionServer{scheduler: sc}
	ctx := context.Background()

	cmd := remoteexecution.Command{Arguments: []string{"/bin/true"}}
	cmdSha, cmdLen, err := scootproto.GetSha256(&cmd)
	if err != nil {
		t.Fatalf("Failed to get sha: %v", err)
	}
	dir := remoteexecution.Directory{}
	dirSha, dirLen, err := scootproto.GetSha256(&dir)
	if err != nil {
		t.Fatalf("Failed to get sha: %v", err)
	}

	a := remoteexecution.Action{
		CommandDigest:   &remoteexecution.Digest{Hash: cmdSha, SizeBytes: cmdLen},
		InputRootDigest: &remoteexecution.Digest{Hash: dirSha, SizeBytes: dirLen},
	}
	req := remoteexecution.ExecuteRequest{
		Action:              &a,
		InstanceName:        "test",
		SkipCacheLookup:     true,
		TotalInputFileCount: 0,
		TotalInputFileBytes: 0,
	}

	res, err := s.Execute(ctx, &req)
	if err != nil {
		t.Fatalf("Non-nil error from Execute: %v", err)
	}

	done := res.GetDone()
	if !done {
		t.Fatal("Expected response to be done")
	}
	metadataAny := res.GetMetadata()
	if metadataAny == nil {
		t.Fatalf("Nil metadata from operation: %s", res)
	}
	execResAny := res.GetResponse()
	if execResAny == nil {
		t.Fatalf("Nil response from operation: %s", res)
	}

	metadata := remoteexecution.ExecuteOperationMetadata{}
	execRes := remoteexecution.ExecuteResponse{}
	err = ptypes.UnmarshalAny(metadataAny, &metadata)
	if err != nil {
		t.Fatalf("Failed to unmarshal metadata from any: %v", err)
	}
	err = ptypes.UnmarshalAny(execResAny, &execRes)
	if err != nil {
		t.Fatalf("Failed to unmarshal response from any: %v", err)
	}
}

// Determine that GetOperation can accept a well-formed request and returns a well-formed response
func TestGetOperationStub(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sc := scheduler.NewMockScheduler(mockCtrl)
	mockSagaLog := saga.NewMockSagaLog(mockCtrl)
	sagaC := saga.MakeSagaCoordinator(mockSagaLog)
	mockSagaLog.EXPECT().GetMessages(gomock.Any()).Return([]saga.SagaMessage{}, nil)

	s := executionServer{
		scheduler: sc,
		sagaCoord: sagaC,
	}
	ctx := context.Background()

	req := longrunning.GetOperationRequest{
		Name: "testJobID",
	}

	res, err := s.GetOperation(ctx, &req)
	if err != nil {
		t.Fatalf("Non-nil error from GetOperation: %v", err)
	}

	done := res.GetDone()
	if !done {
		t.Fatal("Expected response to be done")
	}
	metadataAny := res.GetMetadata()
	if metadataAny == nil {
		t.Fatalf("Nil metadata from operation: %s", res)
	}
	getOpResAny := res.GetResponse()
	if getOpResAny == nil {
		t.Fatalf("Nil response from operation: %s", res)
	}

	metadata := remoteexecution.ExecuteOperationMetadata{}
	getOpRes := remoteexecution.ExecuteResponse{}
	err = ptypes.UnmarshalAny(metadataAny, &metadata)
	if err != nil {
		t.Fatalf("Failed to unmarshal metadata from any: %v", err)
	}
	err = ptypes.UnmarshalAny(getOpResAny, &getOpRes)
	if err != nil {
		t.Fatalf("Failed to unmarshal response from any: %v", err)
	}

}
