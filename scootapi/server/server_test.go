package server

import (
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/scootdev/scoot/saga/sagalogs"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/scootdev/scoot/tests/testhelpers"
)

// ensure a scheduler initializes to the correct state
func Test_RequestCounters(t *testing.T) {

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	s := scheduler.NewMockScheduler(mockCtrl)
	s.EXPECT().ScheduleJob(gomock.Any()).Return("mockJobId", nil)
	s.EXPECT().KillJob(gomock.Any()).Return(nil)
	sc := sagalogs.MakeInMemorySagaCoordinator()
	statsRegistry := stats.NewFinagleStatsRegistry()

	statsReceiver, _ := stats.NewCustomStatsReceiver(func() stats.StatsRegistry{ return statsRegistry}, 0)

	handler := NewHandler(s, sc, statsReceiver)

	domainJobDef := sched.GenJobDef(1)

	scootJobDef, _ := schedJobDefToScootJobDef(&domainJobDef)

	_, err := handler.RunJob(scootJobDef)
	if err != nil {
		t.Errorf("RunJob returned err:%s",err.Error())
	}

	_, err = handler.GetStatus("testJobId")
	if err != nil {
		t.Errorf("GetStatus returned err:%s",err.Error())
	}

	_, err = handler.KillJob("testJobId")
	if err != nil {
		t.Errorf("GetStatus returned err:%s",err.Error())
	}

	testhelpers.VerifyStats(statsRegistry, t,
		map[string]testhelpers.Rule{
			"runJobRpmCounter" : {Checker:testhelpers.Int64EqTest, Value: 1},
			"runJobLatency_ms.avg" : {Checker:testhelpers.FloatGTTest, Value: 0.0},
			"jobStatusRpmCounter" : {Checker:testhelpers.Int64EqTest, Value: 1},
			"jobStatusLatency_ms.avg" : {Checker:testhelpers.FloatGTTest, Value: 0.0},
			"jobKillRpmCounter" : {Checker:testhelpers.Int64EqTest, Value: 1},
			"jobKillLatency_ms.avg" : {Checker:testhelpers.FloatGTTest, Value: 0.0},
		})
}





// converts a scheduler Job into a Thrift Job
func schedJobDefToScootJobDef(schedJobDef *sched.JobDefinition) (*scoot.JobDefinition, error) {
	if schedJobDef == nil {
		return nil, nil
	}

	scootTasks := make(map[string]*scoot.TaskDefinition)
	for taskName, schedTask := range schedJobDef.Tasks {
		cmd := scoot.Command{
			Argv:       schedTask.Argv,
		}
		taskId := schedTask.TaskID
		scootTask := &scoot.TaskDefinition{Command: &cmd, TaskId: &taskId}
		scootTasks[taskName] = scootTask
	}

	unknown := scoot.JobType(scoot.JobType_UNKNOWN)
	scootJobDefinition := scoot.JobDefinition{
		JobType: &unknown,
		Tasks:   scootTasks,
	}

	return &scootJobDefinition, nil
}


