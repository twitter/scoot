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

	"github.com/wisechengyi/scoot/common/log/tags"
	"github.com/wisechengyi/scoot/perftests/scheduler_simulator"
	"github.com/wisechengyi/scoot/runner"
	"github.com/wisechengyi/scoot/scheduler/domain"
	"github.com/wisechengyi/scoot/scheduler/server"
)

func main() {

	log.SetReportCaller(true)
	log.SetLevel(log.WarnLevel)

	var jobDefs map[int][]*domain.JobDefinition

	log.Warn("****************************************")
	log.Warn("********* Building Job Defs ************")
	log.Warn("****************************************")
	testsStart := time.Now()
	var testsEnd time.Time

	jobDefs = makeJobDefs()
	testsEnd = testsStart.Add(30 * time.Second)

	log.Warn("****************************************")
	log.Warn("******** Starting Simulation ***********")
	log.Warn("****************************************")
	schedAlg := scheduler_simulator.MakeSchedulingAlgTester(testsStart, testsEnd, jobDefs, 5, server.DefaultLoadBasedSchedulerClassPercents, server.DefaultRequestorToClassMap)
	e := schedAlg.RunTest()
	if e != nil {
		log.Fatalf("%s\n", e.Error())
	}
}

func makeJobDefs() map[int][]*domain.JobDefinition {
	jd := make([]domain.JobDefinition, 3)

	jd[0].Basis = fmt.Sprintf("%d", 0*time.Second)  // run right away
	jd[1].Basis = fmt.Sprintf("%d", 10*time.Second) // run 10 seconds into the test
	jd[2].Basis = fmt.Sprintf("%d", 20*time.Second) // run 20 seconds into the test

	m := make(map[int][]*domain.JobDefinition)
	for i := 0; i < 3; i++ {
		jd[i].JobType = "fakeJobType"
		jd[i].Requestor = fmt.Sprintf("fakeRequestor%d", i)
		jd[i].Tag = "{url:fake_job, elapsedMin:3}"
		jd[i].Priority = domain.Priority(i)
		jd[i].Tasks = makeFakeTasks()
		t := int(time.Now().Add(time.Duration(-1*i) * time.Minute).Unix())
		m[t] = make([]*domain.JobDefinition, 1)
		m[t][0] = &jd[i]
	}

	return m
}

func makeFakeTasks() []domain.TaskDefinition {
	//cnt := int(rand.Float64() * 10)+ 1
	cnt := 6

	tasks := make([]domain.TaskDefinition, cnt)
	for i := 0; i < cnt; i++ {

		td := runner.Command{
			Argv:       []string{"cmd", "5", "0"}, // fake cmd, sleep time, exit code
			EnvVars:    nil,
			Timeout:    0,
			SnapshotID: "",
			LogTags:    tags.LogTags{TaskID: fmt.Sprintf("%d", i), Tag: "fakeTag"},
		}
		tasks[i] = domain.TaskDefinition{Command: td}
	}

	return tasks
}
