package thrift

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/twitter/scoot/scheduler/server"
)

/*
 */
func Test_GetSchedulerStatus(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	s := server.NewMockScheduler(mockCtrl)
	s.EXPECT().GetSchedulerStatus().Return(20, -1)

	// test scheduler.GetSchedulerStatus
	status, err := GetSchedulerStatus(s)
	if status.CurrentTasks != 20 ||
		status.MaxTasks != -1 {
		t.Fatalf("Expected {20, -1}, got %v", status)
	}

	if err != nil {
		t.Fatalf("Expected nil, got %s", err)
	}
}
