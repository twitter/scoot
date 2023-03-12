package thrift

import (
	"errors"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/wisechengyi/scoot/common/stats"
	"github.com/wisechengyi/scoot/scheduler/api/thrift/gen-go/scoot"
	"github.com/wisechengyi/scoot/scheduler/server"
	"github.com/wisechengyi/scoot/tests/testhelpers"
)

func IsInvalidJobRequest(err error) bool {
	switch err.(type) {
	case *InvalidJobRequest:
		return true
	default:
		return false
	}
}

func CreateSchedulerMock(t *testing.T) *server.MockScheduler {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	scheduler := server.NewMockScheduler(mockCtrl)
	return scheduler
}

// Job with No Taks Should return an InvalidJobRequest error
func Test_RunJob_WithNoTasks(t *testing.T) {
	jobDef := scoot.NewJobDefinition()

	jobId, err := RunJob(CreateSchedulerMock(t), jobDef, stats.NilStatsReceiver())
	if err == nil {
		t.Errorf("expected error running Job with no command")
	}

	if !IsInvalidJobRequest(err) {
		t.Errorf("expected error to be InvalidJobRequest not %v", reflect.TypeOf(err))
	}

	if jobId != nil {
		t.Errorf("expected job Id to be nil when error occurs not %v", jobId)
	}
}

// Jobs with Invalid Task Ids should return an InvalidJobRequest error
func Test_RunJob_InvalidTaskId(t *testing.T) {
	jobDef := scoot.NewJobDefinition()
	task := testhelpers.GenTask(testhelpers.NewRand(), "", "")
	jobDef.Tasks = []*scoot.TaskDefinition{task}
	jobId, err := RunJob(CreateSchedulerMock(t), jobDef, stats.NilStatsReceiver())

	if !IsInvalidJobRequest(err) {
		t.Errorf("expected error to be InvalidJobRequest not %v", reflect.TypeOf(err))
	}

	if jobId != nil {
		t.Errorf("expected job Id to be nil when error occurs not %v", jobId)
	}
}

// Jobs with Tasks with no commands should return InvalidJobRequest error
func Test_RunJob_NoCommand(t *testing.T) {
	jobDef := scoot.NewJobDefinition()
	task := testhelpers.GenTask(testhelpers.NewRand(), "1", "")
	task.Command.Argv = []string{}
	jobDef.Tasks = []*scoot.TaskDefinition{task}
	jobId, err := RunJob(CreateSchedulerMock(t), jobDef, stats.NilStatsReceiver())

	if !IsInvalidJobRequest(err) {
		t.Errorf("expected error to be InvalidJobRequest not %v", reflect.TypeOf(err))
	}

	if jobId != nil {
		t.Errorf("expected job Id to be nil when error occurs not %v", jobId)
	}
}

func Test_RunJob_ValidJob(t *testing.T) {
	jobDef := testhelpers.GenJobDefinition(testhelpers.NewRand(), -1, "")

	scheduler := CreateSchedulerMock(t)
	scheduler.EXPECT().ScheduleJob(gomock.Any()).Return("testJobId", nil)

	jobId, err := RunJob(scheduler, jobDef, stats.NilStatsReceiver())

	if err != nil {
		t.Errorf("expected job to be successfully scheduled.  Instead error returned: %v", err)
	}

	if jobId.ID != "testJobId" {
		t.Errorf("expected jobId to be testJobId not %v", jobId.ID)
	}
}

func Test_RunJob_SchedulerError(t *testing.T) {
	jobDef := testhelpers.GenJobDefinition(testhelpers.NewRand(), -1, "")

	scheduler := CreateSchedulerMock(t)
	scheduler.EXPECT().ScheduleJob(gomock.Any()).Return("", errors.New("test error"))

	jobId, err := RunJob(scheduler, jobDef, stats.NilStatsReceiver())

	if err == nil {
		t.Error("expected error when scheduler returns an error")
	}

	if jobId != nil {
		t.Errorf("expected job Id to be nil when error occurs not %v", jobId)
	}
}
