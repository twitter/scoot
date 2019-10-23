package scheduler

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/sched"
)

func TestSchedulingStandardProgression(t *testing.T) {
	pRatios := [3]int{1, 3, 10}
	pbs := MakePriorityBasedAlg(pRatios[:])

	// 10 jobs, p0 1task,p1 2tasks,p2 3tasks,p0 4tasks,p1 5tasks,...
	jobs := makeJobStates()
	cluster := &clusterState{
		updateCh:         nil,
		nodes:            nil,
		suspendedNodes:   nil,
		offlinedNodes:    nil,
		nodeGroups:       makeIdleGroup(5), // always have 5 idle nodes
		maxLostDuration:  0,
		maxFlakyDuration: 0,
		readyFn:          nil,
		numRunning:       0,
		stats:            nil,
	}
	cluster.nodes = cluster.nodeGroups["idle"].idle
	sc := SchedulerConfig{
		MaxRetriesPerTask:       0,
		DebugMode:               false,
		RecoverJobsOnStartup:    false,
		DefaultTaskTimeout:      0,
		TaskTimeoutOverhead:     0,
		RunnerRetryTimeout:      0,
		RunnerRetryInterval:     0,
		ReadyFnBackoff:          0,
		MaxRequestors:           0,
		MaxJobsPerRequestor:     0,
		SoftMaxSchedulableTasks: 0,
		TaskThrottle:            0,
		Admins:                  nil,
	}

	for i := 0; i < 11; i++ {
		tasks := pbs.GetTasksToBeAssigned(jobs, nil, cluster, nil, sc)

		if len(tasks) != 5 {
			t.Fatalf("expected 5 tasks, got %d:\n%v", len(tasks), tasks)
		}
		moveToCompleted(tasks, jobs)
	}
	tasks := pbs.GetTasksToBeAssigned(jobs, nil, cluster, nil, sc)

	if len(tasks) != 0 {
		t.Fatalf("expected 0 tasks, got %d:\n%v", len(tasks), tasks)
	}
}

func makeJobStates() []*jobState {
	numJobs := 10
	jobStates := make([]*jobState, numJobs)

	for i := 0; i < numJobs; i++ {
		j := &sched.Job{
			Id: fmt.Sprintf("job%d", i),
			Def: sched.JobDefinition{
				JobType:   "dummyJobType",
				Requestor: "",
				Basis:     "",
				Tag:       "",
				Priority:  sched.Priority(int(math.Mod(float64(i), 3))),
			},
		}
		js := &jobState{
			Job:            j,
			Saga:           nil,
			EndingSaga:     false,
			TasksCompleted: 0,
			TasksRunning:   0,
			JobKilled:      false,
			TimeCreated:    time.Time{},
			TimeMarker:     time.Time{},
			Completed:      make(map[string]*taskState),
			Running:        nil,
			NotStarted:     nil,
		}
		t, ts := makeDummyTasks(j.Id, i+1)
		j.Def.Tasks = t
		js.Tasks = ts
		js.NotStarted = makeTaskMap(ts)
		jobStates[i] = js
	}

	return jobStates

}

func makeTaskMap(tasks []*taskState) map[string]*taskState {
	rVal := make(map[string]*taskState)
	for _, t := range tasks {
		rVal[t.TaskId] = t
	}
	return rVal
}

func makeDummyTasks(jobId string, numTasks int) ([]sched.TaskDefinition, []*taskState) {

	tasks := make([]sched.TaskDefinition, int(numTasks))
	tasksState := make([]*taskState, int(numTasks))
	for i := 0; i < numTasks; i++ {

		td := runner.Command{
			Argv:           []string{""},
			EnvVars:        nil,
			Timeout:        0,
			SnapshotID:     "",
			LogTags:        tags.LogTags{TaskID: fmt.Sprintf("%d", i), Tag: "dummyTag"},
			ExecuteRequest: nil,
		}
		tasks[i] = sched.TaskDefinition{td}

		tasksState[i] = &taskState{
			JobId:  jobId,
			TaskId: fmt.Sprintf("task%d", i),
			Status: sched.NotStarted,
		}
	}

	return tasks, tasksState
}

func makeIdleGroup(n int) map[string]*nodeGroup {
	idle := make(map[cluster.NodeId]*nodeState)
	for i := 0; i < n; i++ {
		idle[cluster.NodeId(fmt.Sprintf("node%d", i))] = &nodeState{}
	}
	idleGroup := &nodeGroup{}
	idleGroup.idle = idle

	rVal := make(map[string]*nodeGroup)
	rVal["idle"] = idleGroup
	return rVal
}

func moveToCompleted(tasks []*taskState, jobs []*jobState) {
	for _, task := range tasks {
		jid := task.JobId
		for _, js := range jobs {
			if js.Job.Id == jid {
				js.Completed[task.TaskId] = task
				delete(js.NotStarted, task.TaskId)
			}
		}
	}
}
