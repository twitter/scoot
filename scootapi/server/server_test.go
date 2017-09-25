package server

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/saga/sagalogs"
	"github.com/twitter/scoot/sched"
	"github.com/twitter/scoot/sched/scheduler"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
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

	statsReceiver, _ := stats.NewCustomStatsReceiver(func() stats.StatsRegistry { return statsRegistry }, 0)

	handler := NewHandler(s, sc, statsReceiver)

	domainJobDef := sched.GenJobDef(1)
	domainJobDef.Tasks[0].Argv = []string{}
	if len(domainJobDef.Tasks[0].Argv) == 0 {
		domainJobDef.Tasks[0].Argv = []string{"sampleArg"}
	}

	scootJobDef, _ := schedJobDefToScootAPIThriftJobDef(&domainJobDef)

	_, err := handler.RunJob(scootJobDef)
	if err != nil {
		t.Errorf("RunJob returned err:%s", err.Error())
	}

	_, err = handler.GetStatus("testJobId")
	if err != nil {
		t.Errorf("GetStatus returned err:%s", err.Error())
	}

	_, err = handler.KillJob("testJobId")
	if err != nil {
		t.Errorf("GetStatus returned err:%s", err.Error())
	}

	time.Sleep(stats.StatReportIntvl + (10 * time.Millisecond)) // wait to make sure stats are generated
	if !stats.StatsOk("", statsRegistry, t,
		map[string]stats.Rule{
			stats.SchedServerStartedGauge:                 {Checker: stats.Int64EqTest, Value: 1},
			stats.SchedServerRunJobCounter:                {Checker: stats.Int64EqTest, Value: 1},
			stats.SchedServerRunJobLatency_ms + ".avg":    {Checker: stats.FloatGTTest, Value: 0.0},
			stats.SchedServerJobStatusCounter:             {Checker: stats.Int64EqTest, Value: 1},
			stats.SchedServerJobStatusLatency_ms + ".avg": {Checker: stats.FloatGTTest, Value: 0.0},
			stats.SchedServerJobKillCounter:               {Checker: stats.Int64EqTest, Value: 1},
			stats.SchedServerJobKillLatency_ms + ".avg":   {Checker: stats.FloatGTTest, Value: 0.0},
			stats.SchedUptime_ms:                          {Checker: stats.Int64GTTest, Value: 0},
		}) {
		t.Fatal("stats check did not pass.")
	}
}

/*
TODO - reduce the number of JobDefinition structures in the platform!
converts a scheduler JobDefinition into a scootapi Thrift JobDefinition.  Note: there are 3 JobDefinitions:
sched.JobDefinition - the domain structure used through the scheduler implementation
scoot.JobDefinition - the thrift structure created by scootapi's thrift definition
schedthrift.JobDefinition - the thrift structure created by the scheduler's thrift definition
(plus there is CLIJobDef which looks a lot like JobDefinition, but is only generated from the CLI's
input job definition json file)

At this point this functionality is only needed for testing because we are reusing the JobDefinition
generator.
*/
func schedJobDefToScootAPIThriftJobDef(schedJobDef *sched.JobDefinition) (*scoot.JobDefinition, error) {
	if schedJobDef == nil {
		return nil, nil
	}

	scootTasks := []*scoot.TaskDefinition{}
	for _, schedTask := range schedJobDef.Tasks {
		cmd := scoot.Command{
			Argv: schedTask.Argv,
		}
		taskId := schedTask.TaskID
		scootTask := &scoot.TaskDefinition{Command: &cmd, TaskId: &taskId}
		scootTasks = append(scootTasks, scootTask)
	}

	unknown := ""
	scootJobDefinition := scoot.JobDefinition{
		JobType: &unknown,
		Tasks:   scootTasks,
	}

	return &scootJobDefinition, nil
}
