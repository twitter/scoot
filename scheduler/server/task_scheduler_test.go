package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twitter/scoot/cloud/cluster"
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

	saga, _ := sagalogs.MakeInMemorySagaCoordinatorNoGC().MakeSaga(job.Id, jobAsBytes)
	js := newJobState(&job, "", saga, nil, nil)

	// create a test cluster with no nodes
	testCluster := makeTestCluster()
	s := getDebugStatefulScheduler(testCluster)
	assignments, _ := getTaskAssignments(testCluster, []*jobState{js}, s)

	if len(assignments) != 0 {
		t.Errorf("Assignments on a cluster with no nodes should not return any assignments")
	}
}

func Test_TaskAssignment_NoTasks(t *testing.T) {
	// create a test cluster with no nodes
	testCluster := makeTestCluster("node1", "node2", "node3", "node4", "node5")
	s := getDebugStatefulScheduler(testCluster)
	assignments, _ := getTaskAssignments(testCluster, []*jobState{}, s)

	if len(assignments) != 0 {
		t.Errorf("Assignments on a cluster with no nodes should not return any assignments")
	}
}

// Currently we schedule based on availability only.  This
// Test verifies that tasks are scheduled on all available nodes.
func Test_TaskAssignments_TasksScheduled(t *testing.T) {
	job := domain.GenJob(testhelpers.GenJobId(testhelpers.NewRand()), 10)
	jobAsBytes, _ := job.Serialize()

	saga, _ := sagalogs.MakeInMemorySagaCoordinatorNoGC().MakeSaga(job.Id, jobAsBytes)
	js := newJobState(&job, "", saga, nil, nil)

	// create a test cluster with no nodes
	testCluster := makeTestCluster("node1", "node2", "node3", "node4", "node5")
	s := getDebugStatefulScheduler(testCluster)
	unScheduledTasks := js.getUnScheduledTasks()
	assignments, _ := getTaskAssignments(testCluster, []*jobState{js}, s)

	if len(assignments) != min(len(unScheduledTasks), len(testCluster.nodes)) {
		t.Errorf(`Expected as many tasks as possible to be scheduled: NumScheduled %v, 
      Number Of Available Nodes %v, Number of Unscheduled Tasks %v`,
			len(assignments),
			len(testCluster.nodes),
			len(unScheduledTasks))
	}
}

func Test_TaskAssignment_Affinity(t *testing.T) {
	testCluster := makeTestCluster("node1", "node2")
	s := getDebugStatefulScheduler(testCluster)
	cs := newClusterState(testCluster.nodes, testCluster.ch, nil, stats.NilStatsReceiver())
	// put the tasks in different jobs to make sure the first assignment has 1 task from job1 and 1 from job2
	// giving us tasks with different snapshotIDs
	j1Tasks := []*taskState{
		{TaskId: "task1", JobId: "job1", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
		{TaskId: "task2", JobId: "job1", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
	}
	j1s := &jobState{Job: &domain.Job{Id: "job1"}, Tasks: j1Tasks, Running: make(map[string]*taskState),
		Completed: make(map[string]*taskState), NotStarted: map[string]*taskState{"task1": j1Tasks[0], "task2": j1Tasks[1]}}

	j2Tasks := []*taskState{
		{TaskId: "task3", JobId: "job2", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapB"}}},
		{TaskId: "task4", JobId: "job2", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapB"}}},
	}
	j2s := &jobState{Job: &domain.Job{Id: "job2"}, Tasks: j2Tasks, Running: make(map[string]*taskState),
		Completed: make(map[string]*taskState), NotStarted: map[string]*taskState{"task3": j2Tasks[0], "task4": j2Tasks[1]}}

	assignments, _ := getTaskAssignments(testCluster, []*jobState{j1s, j2s}, s)
	if len(assignments) != 2 {
		t.Errorf("Expected first three tasks to be assigned, got %v", len(assignments))
	}

	// Schedule the first two tasks (one from each job) and complete the task from job1.
	taskNodes := map[string]cluster.NodeId{}
	for _, as := range assignments {
		taskNodes[as.task.TaskId] = as.nodeSt.node.Id()
		cs.taskScheduled(as.nodeSt.node.Id(), as.task.JobId, as.task.TaskId, as.task.Def.SnapshotID)
		if as.task.JobId == "job1" {
			j1s.taskStarted(as.task.TaskId, &taskRunner{})
			cs.taskCompleted(as.nodeSt.node.Id(), false)
			j1s.taskCompleted(as.task.TaskId, true)
		} else {
			j2s.taskStarted(as.task.TaskId, &taskRunner{})
		}
	}

	// Add a new idle node, assign tasks and confirm that job1's task is assigned to
	// the same node as the prior job1 task
	testCluster.add("node3")
	assignments, _ = getTaskAssignments(testCluster, []*jobState{j1s, j2s}, s)
	if len(assignments) != 2 {
		t.Errorf("Expected 2 tasks to be assigned, got %v", len(assignments))
	}

	for _, as := range assignments {
		if as.task.TaskId == "task2" {
			assert.Equal(t, fmt.Sprintf("%s", taskNodes["task1"]), fmt.Sprintf("%s", as.nodeSt.node.Id()), "expected task2 to be assigned to task1's node")
		}
	}
}

func getDebugStatefulScheduler(tc *testCluster) *statefulScheduler {
	rfn := func() stats.StatsRegistry { return stats.NewFinagleStatsRegistry() }
	statsReceiver, _ := stats.NewCustomStatsReceiver(rfn, 0)
	rf := func(n cluster.Node) runner.Service {
		return worker.MakeInmemoryWorker(n, tmp)
	}
	sc := SchedulerConfig{
		MaxRetriesPerTask:    0,
		DebugMode:            true,
		RecoverJobsOnStartup: false,
		DefaultTaskTimeout:   time.Second,
	}
	s := NewStatefulScheduler(tc.nodes, tc.ch,
		sagalogs.MakeInMemorySagaCoordinatorNoGC(),
		rf,
		sc,
		statsReceiver,
	)
	return s
}

func getTaskAssignments(tc *testCluster, js []*jobState, s *statefulScheduler) ([]taskAssignment, map[string]*nodeGroup) {

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
