package api

import (
	"testing"

	"github.com/twitter/scoot/sched/scheduler"
	"github.com/golang/mock/gomock"
)

/*
*/
func Test_GetSchedulerStatus(t *testing.T) {

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	s := scheduler.NewMockScheduler(mockCtrl)
	s.EXPECT().GetSchedulerStatus().Return(true, 20, -1)

	// test scheduler.KillJob returning a non-null error
	status, err := GetSchedulerStatus(s)
	if status.ReceivingJobs != true ||
		status.CurrentTasks != 20 ||
		status.MaxTasks != -1 {
		t.Fatalf("Expected {true, 20, -1}, got %v",status)
	}

	if err != nil {
		t.Fatalf("Expected nil, got %s",err)
	}
}


