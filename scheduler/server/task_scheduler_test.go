package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twitter/scoot/cloud/cluster"
	cc "github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/saga/sagalogs"
	"github.com/twitter/scoot/scheduler/domain"
	"github.com/twitter/scoot/scheduler/setup/worker"
	"github.com/twitter/scoot/tests/testhelpers"
)

func Test_TaskAssignment_NoNodesAvailable(t *testing.T) {
	job := domain.GenJob(testhelpers.GenJobId(testhelpers.NewRand()), 10)
	jobAsBytes, _ := job.Serialize()

	saga, _ := sagalogs.MakeInMemorySagaCoordinatorNoGC(nil).MakeSaga(job.Id, jobAsBytes)
	js := newJobState(&job, "", saga, nil, nil, nopDurationKeyExtractor)

	// create a test cluster with no nodes
	uc := make(chan []cc.NodeUpdate)
	s := getDebugStatefulScheduler(uc)
	assignments := getTaskAssignments([]*jobState{js}, s)

	if len(assignments) != 0 {
		t.Errorf("Assignments on a cluster with no nodes should not return any assignments")
	}
}

func Test_TaskAssignment_NoTasks(t *testing.T) {
	// create a test cluster with 5 nodes
	uc := initNodeUpdateChan("node1", "node2", "node3", "node4", "node5")
	s := getDebugStatefulScheduler(uc)
	assignments := getTaskAssignments([]*jobState{}, s)

	if len(assignments) != 0 {
		t.Errorf("Assignments on a cluster with no nodes should not return any assignments")
	}
}

// Currently we schedule based on availability only.  This
// Test verifies that tasks are scheduled on all available nodes.
func Test_TaskAssignments_TasksScheduled(t *testing.T) {
	job := domain.GenJob(testhelpers.GenJobId(testhelpers.NewRand()), 10)
	jobAsBytes, _ := job.Serialize()

	saga, _ := sagalogs.MakeInMemorySagaCoordinatorNoGC(nil).MakeSaga(job.Id, jobAsBytes)
	js := newJobState(&job, "", saga, nil, nil, nopDurationKeyExtractor)

	// create a test cluster with 5 nodes
	uc := initNodeUpdateChan("node1", "node2", "node3", "node4", "node5")
	s := getDebugStatefulScheduler(uc)
	unScheduledTasks := js.getUnScheduledTasks()
	assignments := getTaskAssignments([]*jobState{js}, s)

	if len(assignments) != min(len(unScheduledTasks), 5) {
		t.Errorf(`Expected as many tasks as possible to be scheduled: NumScheduled %v, 
      5 nodes available, Number of Unscheduled Tasks %v`,
			len(assignments),
			len(unScheduledTasks))
	}
}

func Test_TaskAssignment_Affinity(t *testing.T) {
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC(nil)
	s, _, _ := initializeServices(sc, false)
	cs := s.clusterState
	// put the tasks in different jobs to make sure the first assignment has 1 task from job1 and 1 from job2
	// giving us tasks with different snapshotIDs
	j1Tasks := []*taskState{
		{TaskId: "task11", JobId: "job1", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
		{TaskId: "task12", JobId: "job1", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
		{TaskId: "task13", JobId: "job1", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
		{TaskId: "task14", JobId: "job1", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
	}
	j1s := &jobState{Job: &domain.Job{Id: "job1"}, Tasks: j1Tasks}

	j2Tasks := []*taskState{
		{TaskId: "task21", JobId: "job2", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapB"}}},
		{TaskId: "task22", JobId: "job2", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapB"}}},
		{TaskId: "task23", JobId: "job2", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapB"}}},
		{TaskId: "task24", JobId: "job2", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapB"}}},
	}
	j2s := &jobState{Job: &domain.Job{Id: "job2"}, Tasks: j2Tasks}

	assignments1 := getTaskAssignments([]*jobState{j1s, j2s}, s)
	if len(assignments1) != 5 {
		t.Errorf("Expected 5 tasks to be assigned, got %v", len(assignments1))
	}

	_, ok := cs.nodeGroups[""]
	assert.True(t, ok, "didn't find '' nodeGroup")

	// complete one task from each job.
	taskNodes := map[string]cluster.NodeId{}
	completedTasksByJob := map[string]string{}
	for _, as := range assignments1 {
		taskNodes[as.task.TaskId] = as.nodeSt.node.Id()
		job := j1s
		if as.task.JobId == "job2" {
			job = j2s
		}
		job.taskStarted(as.task.TaskId, &taskRunner{nodeSt: as.nodeSt})
		if _, ok := completedTasksByJob[as.task.JobId]; !ok {
			cs.taskCompleted(as.nodeSt.node.Id(), false)
			job.taskCompleted(as.task.TaskId, true)
			completedTasksByJob[as.task.JobId] = as.task.TaskId
		}
	}

	// run a second task assignment, the task should go to the node with the matching snapshot id
	assignments2 := getTaskAssignments([]*jobState{j1s, j2s}, s)
	if len(assignments2) != 2 {
		t.Errorf("Expected 2 tasks to be assigned, got %v", len(assignments2))
	}

	// check the task assignment is to the correct node
	haveNodeMatch := false
	for _, as2 := range assignments2 {
		for _, as1 := range assignments1 {
			if as2.task.JobId == as1.task.JobId && as2.nodeSt.node.Id() == as1.nodeSt.node.Id() {
				haveNodeMatch = true
				break
			}
		}
	}

	assert.True(t, haveNodeMatch)
	_, ok = cs.nodeGroups[""]
	assert.True(t, ok, "didn't find '' nodeGroup")
}

func getDebugStatefulScheduler(uc chan []cc.NodeUpdate) *statefulScheduler {
	rfn := func() stats.StatsRegistry { return stats.NewFinagleStatsRegistry() }
	statsReceiver, _ := stats.NewCustomStatsReceiver(rfn, 0)
	rf := func(n cluster.Node) runner.Service {
		return worker.MakeInmemoryWorker(n)
	}
	sc := SchedulerConfiguration{
		MaxRetriesPerTask:    0,
		DebugMode:            true,
		RecoverJobsOnStartup: false,
		DefaultTaskTimeout:   time.Second,
	}
	s := NewStatefulScheduler(uc,
		sagalogs.MakeInMemorySagaCoordinatorNoGC(nil),
		rf,
		sc,
		statsReceiver,
		nil,
		nil,
	)
	return s
}

func getTaskAssignments(js []*jobState, s *statefulScheduler) []taskAssignment {

	s.inProgressJobs = js
	reqMap := map[string][]*jobState{}
	for _, j := range js {
		if _, ok := reqMap[j.Job.Def.Requestor]; !ok {
			reqMap[j.Job.Def.Requestor] = []*jobState{}
		}
		reqMap[j.Job.Def.Requestor] = append(reqMap[j.Job.Def.Requestor], j)
		j.tasksByJobClassAndStartTimeSec = s.tasksByJobClassAndStartTimeSec
	}
	s.requestorMap = reqMap

	return s.getTaskAssignments()
}
