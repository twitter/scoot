package main

/*
This is a cli entry for the scheduling algorithm evaluation framework.

The main builds a set of 5 'shadow' job definitions for testing the overall framework.

sample run command:
workspace$ ./bin/priority_scheduling_sim

*/
import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/perftests/scheduler_simulator"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/sched"
)

func main() {

	log.SetReportCaller(true)
	log.SetLevel(log.WarnLevel)

	var jobDefs map[int][]*sched.JobDefinition
	var pRatios []int

	log.Warn("****************************************")
	log.Warn("********* Building Job Defs ************")
	log.Warn("****************************************")
	testsStart := time.Now()
	var testsEnd time.Time
	t := [...]int{1, 5, 3}
	pRatios = t[:]

	jobDefs = makeJobDefs()
	testsEnd = testsStart.Add(30 * time.Second)

	log.Warn("****************************************")
	log.Warn("******** Starting Simulation ***********")
	log.Warn("****************************************")
	schedAlg := scheduler_simulator.MakeSchedulingAlgTester(testsStart, testsEnd, jobDefs, pRatios[:], 5)
	e := schedAlg.RunTest()
	if e != nil {
		fmt.Printf("%s\n", e.Error())
	}
}

func makeJobDefs() map[int][]*sched.JobDefinition {
	jd := make([]sched.JobDefinition, 3)

	jd[0].Basis = fmt.Sprintf("%d", 0*time.Second)  // run right away
	jd[1].Basis = fmt.Sprintf("%d", 10*time.Second) // run 10 seconds into the test
	jd[2].Basis = fmt.Sprintf("%d", 20*time.Second) // run 20 seconds into the test

	m := make(map[int][]*sched.JobDefinition)
	for i := 0; i < 3; i++ {
		jd[i].JobType = "dummyJobType"
		jd[i].Requestor = fmt.Sprintf("dummyRequestor%d", i)
		jd[i].Tag = "{url:dummy_job, elapsedMin:3}"
		jd[i].Priority = sched.Priority(i)
		jd[i].Tasks = makeDummyTasks()
		t := int(time.Now().Add(time.Duration(-1*i) * time.Minute).Unix())
		m[t] = make([]*sched.JobDefinition, 1)
		m[t][0] = &jd[i]
	}

	return m
}

func makeDummyTasks() []sched.TaskDefinition {
	//cnt := int(rand.Float64() * 10)+ 1
	cnt := 6

	tasks := make([]sched.TaskDefinition, cnt)
	for i := 0; i < cnt; i++ {

		td := runner.Command{
			Argv:           []string{"cmd", "5", "0"}, // dummy cmd, sleep time, exit code
			EnvVars:        nil,
			Timeout:        0,
			SnapshotID:     "",
			LogTags:        tags.LogTags{TaskID: fmt.Sprintf("%d", i), Tag: "dummyTag"},
			ExecuteRequest: nil,
		}
		tasks[i] = sched.TaskDefinition{Command: td}
	}

	return tasks
}
