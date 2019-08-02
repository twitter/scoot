package execution

import (
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"

	scootproto "github.com/twitter/scoot/common/proto"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/sched/scheduler"
)

// Determine that Execute can accept a well-formed request and returns a well-formed response
func TestExecute(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sc := scheduler.NewMockScheduler(mockCtrl)
	sc.EXPECT().ScheduleJob(gomock.Any()).Return("testJobID", nil)

	s := executionServer{scheduler: sc, stat: stats.NilStatsReceiver()}

	fs := &fakeExecServer{}

	a := &remoteexecution.Action{}
	actionSha, actionLen, err := scootproto.GetSha256(a)
	if err != nil {
		t.Fatalf("Failed to get sha: %v", err)
	}

	actionDigest := &remoteexecution.Digest{Hash: actionSha, SizeBytes: actionLen}

	req := remoteexecution.ExecuteRequest{
		InstanceName:    "test",
		SkipCacheLookup: true,
		ActionDigest:    actionDigest,
	}

	err = s.Execute(&req, fs)
	if err != nil {
		t.Fatalf("Non-nil error from Execute: %v", err)
	}
}

// Determine that GetOperation can accept a well-formed request and returns a well-formed response
func TestGetOperation(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sc := scheduler.NewMockScheduler(mockCtrl)
	mockSagaLog := saga.NewMockSagaLog(mockCtrl)
	sagaC := saga.MakeSagaCoordinator(mockSagaLog)
	mockSagaLog.EXPECT().GetMessages(gomock.Any()).Return([]saga.SagaMessage{}, nil)

	s := executionServer{
		scheduler: sc,
		sagaCoord: sagaC,
		stat:      stats.NilStatsReceiver(),
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
	if done {
		t.Fatal("Expected response to not be done")
	}
	metadataAny := res.GetMetadata()
	if metadataAny == nil {
		t.Fatalf("Nil metadata from operation: %s", res)
	}
	if res.GetResponse() != nil {
		t.Fatalf("Non-nil response for incomplete task from operation: %s", res)
	}

	metadata := remoteexecution.ExecuteOperationMetadata{}
	err = ptypes.UnmarshalAny(metadataAny, &metadata)
	if err != nil {
		t.Fatalf("Failed to unmarshal metadata from any: %v", err)
	}
}

// Determine that CancelOperation can accept a well-formed request and returns a well-formed response
func TestCancelOperation(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	sc := scheduler.NewMockScheduler(mockCtrl)
	mockSagaLog := saga.NewMockSagaLog(mockCtrl)
	sagaC := saga.MakeSagaCoordinator(mockSagaLog)
	var wg sync.WaitGroup
	wg.Add(1)
	sc.EXPECT().KillJob(gomock.Any()).Return(nil).Do(func(interface{}) { wg.Done() })
	mockSagaLog.EXPECT().GetMessages(gomock.Any()).Return([]saga.SagaMessage{}, nil)

	s := executionServer{
		scheduler: sc,
		sagaCoord: sagaC,
		stat:      stats.NilStatsReceiver(),
	}
	ctx := context.Background()

	req := longrunning.CancelOperationRequest{
		Name: "testJobID",
	}

	_, err := s.CancelOperation(ctx, &req)
	if err != nil {
		t.Fatalf("Non-nil error from CancelOperation: %v", err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Timeout while attempting to cancel job. Likely means wg.Wait() was never called")
	}
}

// Fake Execution_ExecuteServer
// Implements Execution_ExecuteServer interface
type fakeExecServer struct {
	grpc.ServerStream
}

func (s *fakeExecServer) Send(op *longrunning.Operation) error {
	return nil
}
