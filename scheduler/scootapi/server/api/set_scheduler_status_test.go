package api

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/twitter/scoot/scheduler/sched/scheduler"
)

func Test_SetSchedulerStatus(t *testing.T) {

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	s := scheduler.NewMockScheduler(mockCtrl)
	s.EXPECT().SetSchedulerStatus(10).Return(nil)

	err := SetSchedulerStatus(s, 10)

	if err != nil {
		t.Fatalf("Expected nil, got: %s", err)
	}
}
