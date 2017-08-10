package server

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/sched/scheduler"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
)

var mockCtrl *gomock.Controller

/*
note: this is a weak test, it validates that a call to KillJob() goes into a scheduler
and sagaCoordinator only.  It would be better to validate that the return status shows
the expected JobStatus of job completed with the expected tasks aborted, but that is
difficult to do with this test being in the server package and stateful_scheduler being
private to the scheduler package
*/
func Test_KillJob(t *testing.T) {

	sc := makeMockSagaCoordinator(t)
	s := makeMockScheduler(t)

	// test scheduler.KillJob returning a non-null error
	_, err := KillJob("err", s, sc)
	if err == nil {
		t.Fatal("Expected error insted got nil")
	}

	// test scheduler.KillJob not finding an error and KillJob calling GetJobStatus
	st, err := KillJob("1", s, sc)
	if err != nil {
		t.Fatalf("Expected error to be nil, instead got %s", err.Error())
	}
	if st.Status != scoot.Status_COMPLETED {
		t.Fatalf("Expected status to be completed, instead got %s", st.Status.String())
	}

}

func makeMockSagaCoordinator(t *testing.T) saga.SagaCoordinator {
	mockCtrl = gomock.NewController(t)

	// make a mock sagaLog that always returns a sagaStart and sagaEnd message.
	// This will trigger the saga coordinator to return a saga showing the job as completed
	sagaLogMock := saga.NewMockSagaLog(mockCtrl)
	sm := []saga.SagaMessage{{SagaId: "1", MsgType: saga.StartSaga}, {SagaId: "1", MsgType: saga.EndSaga}}
	sagaLogMock.EXPECT().GetMessages(gomock.Any()).Return(sm, nil)

	return saga.MakeSagaCoordinator(sagaLogMock)
}

func makeMockScheduler(t *testing.T) *scheduler.MockScheduler {

	scheduler := scheduler.NewMockScheduler(mockCtrl)
	scheduler.EXPECT().KillJob("err").Return(fmt.Errorf("saw kil job request in scheduler"))
	scheduler.EXPECT().KillJob("1").Return(nil)

	return scheduler
}
