package server

import (
	"errors"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/scootdev/scoot/tests/testhelpers"
)

func IsInvalidJobRequest(err error) bool {
	switch err.(type) {
	case *InvalidJobRequest:
		return true
	default:
		return false
	}
}

func CreateSchedulerMock(t *testing.T) *scheduler.MockScheduler {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	scheduler := scheduler.NewMockScheduler(mockCtrl)
	return scheduler
}

// Job with No Taks Should return an InvalidJobRequest error
func Test_RunJob_WithNoTasks(t *testing.T) {
	jobDef := scoot.NewJobDefinition()

	jobId, err := runJob(CreateSchedulerMock(t), jobDef)
	if err == nil {
		t.Errorf("expected error running Job with no command")
	}

	if !IsInvalidJobRequest(err) {
		t.Errorf("expected error to be InvalidJobRequest not %v", reflect.TypeOf(err))
	}

	if jobId != nil {
		t.Errorf("expected jobId to be nil when error occurs")
	}
}

// Jobs with Invalid Task Ids should return an InvalidJobRequest error
func Test_RunJob_InvalidTaskId(t *testing.T) {
	jobDef := scoot.NewJobDefinition()
	task := testhelpers.GenTask(testhelpers.NewRand())
	jobDef.Tasks = map[string]*scoot.TaskDefinition{
		"": task,
	}

	jobId, err := runJob(CreateSchedulerMock(t), jobDef)

	if !IsInvalidJobRequest(err) {
		t.Errorf("expected error to be InvalidJobRequest not %v", reflect.TypeOf(err))
	}

	if jobId != nil {
		t.Errorf("expected jobId to be nil when error occurs")
	}
}

// Jobs with Tasks with no commands should return InvalidJobRequest error
func Test_RunJob_NoCommand(t *testing.T) {
	jobDef := scoot.NewJobDefinition()
	task := testhelpers.GenTask(testhelpers.NewRand())
	task.Command.Argv = []string{}
	jobDef.Tasks = map[string]*scoot.TaskDefinition{
		"1": task,
	}

	jobId, err := runJob(CreateSchedulerMock(t), jobDef)

	if !IsInvalidJobRequest(err) {
		t.Errorf("expected error to be InvalidJobRequest not %v", reflect.TypeOf(err))
	}

	if jobId != nil {
		t.Error("expected jobId to be nil when error occurs")
	}
}

func Test_RunJob_ValidJob(t *testing.T) {
	jobDef := testhelpers.GenJobDefinition(testhelpers.NewRand())

	scheduler := CreateSchedulerMock(t)
	scheduler.EXPECT().ScheduleJob(gomock.Any()).Return(nil)

	_, err := runJob(scheduler, jobDef)

	if err != nil {
		t.Errorf("expected job to be successfully scheduled.  Instead error returned: %v", err)
	}
}

func Test_RunJob_SchedulerError(t *testing.T) {
	jobDef := testhelpers.GenJobDefinition(testhelpers.NewRand())

	scheduler := CreateSchedulerMock(t)
	scheduler.EXPECT().ScheduleJob(gomock.Any()).Return(errors.New("test error"))

	_, err := runJob(scheduler, jobDef)

	if err == nil {
		t.Error("expected error when scheduler returns an error")
	}
}
