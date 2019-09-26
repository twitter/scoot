package api

import (
	"strings"
	"testing"

	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/twitter/scoot/scheduler/sched/scheduler"
)

/*
 */
func Test_SetSchedulerStatus(t *testing.T) {

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	s := scheduler.NewMockScheduler(mockCtrl)
	s.EXPECT().SetSchedulerStatus(10).Return(nil)

	// test scheduler.SetSchedulerStatus returning a non-null error
	err := SetSchedulerStatus(s, 10)

	if err != nil {
		t.Fatalf("Expected nil, got: %s", err)
	}
	// test scheduler.SetSchedulerStatus
	err = SetSchedulerStatus(s, -3)

	if strings.Compare(fmt.Sprintf("%v", err), "Error task limit must be > -1") != 0 {
		t.Fatalf("Expected error, got: %s", err)
	}
}
