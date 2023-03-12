package thrift

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/wisechengyi/scoot/scheduler/server"
)

func Test_SetSchedulerStatus(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	s := server.NewMockScheduler(mockCtrl)
	s.EXPECT().SetSchedulerStatus(10).Return(nil)

	// test scheduler.SetSchedulerStatus returning a non-null error
	err := SetSchedulerStatus(s, 10)

	if err != nil {
		t.Fatalf("Expected nil, got: %s", err)
	}
}
