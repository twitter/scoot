package server

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	log "github.com/sirupsen/logrus"
	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/saga/sagalogs"
	"github.com/twitter/scoot/scheduler/domain"
	"github.com/twitter/scoot/scheduler/setup/worker"
)

// We want to see three tasks with TagX scheduled first, followed by one TagY, then the final TagX
func Test_TaskAssignments_RequestorBatching(t *testing.T) {
	js := []*jobState{
		{
			Job: &domain.Job{
				Id:  "job1",
				Def: domain.JobDefinition{Tag: "TagX"},
			},
			Tasks: []*taskState{
				{JobId: "job1", TaskId: "task1", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
				{JobId: "job1", TaskId: "task2", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
			},
		},
		{
			Job: &domain.Job{
				Id:  "job2",
				Def: domain.JobDefinition{Tag: "TagY"},
			},
			Tasks: []*taskState{
				{JobId: "job2", TaskId: "task1", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
			},
		},
		{
			Job: &domain.Job{
				Id:  "job3",
				Def: domain.JobDefinition{Tag: "TagX"},
			},
			Tasks: []*taskState{
				{JobId: "job3", TaskId: "task1", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
				{JobId: "job3", TaskId: "task2", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
			},
		},
	}
	js[0].NotStarted = map[string]*taskState{"task1": js[0].Tasks[0], "task2": js[0].Tasks[1]}
	js[1].NotStarted = map[string]*taskState{"task1": js[1].Tasks[0]}
	js[2].NotStarted = map[string]*taskState{"task1": js[2].Tasks[0], "task2": js[2].Tasks[1]}

	nodes := []string{}
	for i := 0; i < 6; i++ {
		nodes = append(nodes, fmt.Sprintf("node%d", i))
	}
	testCluster := makeTestCluster(nodes...)

	nodeScaleAdjustment := []float32{1, 1, 1}
	config := &OrigSchedulingAlgConfig{
		SoftMaxSchedulableTasks: 10, // We want numTasks*GetNodeScaleFactor()==3 to define a specific order for scheduling.
		NodeScaleAdjustment:     nodeScaleAdjustment,
	}
	assignments := getOrigShedAlgTaskAssignments(testCluster, js, config)
	if len(assignments) != 5 {
		t.Errorf("Expected all five tasks to be assigned, got %v", len(assignments))
	}
	if assignments[0].task.JobId != "job3" || assignments[0].task.TaskId != "task1" {
		t.Errorf("Expected 0:job1.task1, got: %v", spew.Sdump(assignments[0]))
	}
	if assignments[1].task.JobId != "job3" || assignments[1].task.TaskId != "task2" {
		t.Errorf("Expected 1:job1.task2, got: %v", spew.Sdump(assignments[1]))
	}
	if assignments[2].task.JobId != "job1" || assignments[2].task.TaskId != "task1" {
		t.Errorf("Expected 2:job3.task1, got: %v", spew.Sdump(assignments[2]))
	}
	if assignments[3].task.JobId != "job2" || assignments[3].task.TaskId != "task1" {
		t.Errorf("Expected 3:job2.task1, got: %v", spew.Sdump(assignments[3]))
	}
	if assignments[4].task.JobId != "job1" || assignments[4].task.TaskId != "task2" {
		t.Errorf("Expected 4:job3.task2, got: %v", spew.Sdump(assignments[4]))
	}
}

/*
Add Job1.P0, Job2.P1 Job3.P2, Job4.P0
With 3 nodes, expect: scheduled Job3, Job2, Job1
*/
func Test_TaskAssignments_PrioritySimple(t *testing.T) {
	makeJob := func(jobId string, prio domain.Priority) *domain.Job {
		return &domain.Job{Id: jobId, Def: domain.JobDefinition{Priority: prio, Tag: jobId}}
	}
	makeTasks := func(jobId string) []*taskState {
		return []*taskState{
			{JobId: jobId, TaskId: "task1", Def: domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
		}
	}
	js := []*jobState{
		{
			Job:       makeJob("job1", domain.P0),
			Tasks:     makeTasks("job1"),
			Running:   make(map[string]*taskState),
			Completed: make(map[string]*taskState),
		},
		{
			Job:       makeJob("job2", domain.P1),
			Tasks:     makeTasks("job2"),
			Running:   make(map[string]*taskState),
			Completed: make(map[string]*taskState),
		},
		{
			Job:       makeJob("job3", domain.P2),
			Tasks:     makeTasks("job3"),
			Running:   make(map[string]*taskState),
			Completed: make(map[string]*taskState),
		},
		{
			Job:       makeJob("job4", domain.P0),
			Tasks:     makeTasks("job4"),
			Running:   make(map[string]*taskState),
			Completed: make(map[string]*taskState),
		},
	}
	for _, j := range js {
		nsMap := map[string]*taskState{}
		for _, t := range j.Tasks {
			nsMap[t.TaskId] = t
		}
		j.NotStarted = nsMap
	}

	numNodes := 3
	nodes := []string{}
	for i := 0; i < numNodes; i++ {
		nodes = append(nodes, fmt.Sprintf("node%d", i))
	}
	testCluster := makeTestCluster(nodes...)

	assignments := getOrigShedAlgTaskAssignments(testCluster, js, nil)
	if len(assignments) != numNodes {
		t.Errorf("Expected %d tasks to be assigned, got %d", numNodes, len(assignments))
	}
	if assignments[0].task.JobId != "job3" {
		t.Errorf("Expected 0:job3: %v", spew.Sdump(assignments[0]))
	}
	if assignments[1].task.JobId != "job2" {
		t.Errorf("Expected 1:job2, got: %v", spew.Sdump(assignments[1]))
	}
	if assignments[2].task.JobId != "job1" {
		t.Errorf("Expected 2:job1, got: %v", spew.Sdump(assignments[2]))
	}

	// Complete first job and get remaining P0 scheduled
	js[2].taskCompleted(assignments[0].task.TaskId, true)
	assignments = getOrigShedAlgTaskAssignments(testCluster, js[2:], nil)

	if len(assignments) != 1 {
		t.Errorf("Expected additional assignment after previous completion, got: %d", len(assignments))
	}
	if assignments[0].task.JobId != "job4" {
		t.Errorf("Expected 0:job4, got: %v", spew.Sdump(assignments[0]))
	}
}

/*
Set NodeScaleFactor=.2 (10 NumConfiguredNodes / 50 SoftMaxSchedulableTasks) to get the following scheduling.
Add jobs: (10 P2 Tasks), (10 P1 Tasks), (10 P0 Tasks)
With 10 nodes: assign nodes for 7 P2, 2 P1, and 1 P0 tasks
*/
func Test_TaskAssignments_PriorityStages(t *testing.T) {
	makeJob := func(jobId string, prio domain.Priority) *domain.Job {
		return &domain.Job{Id: jobId, Def: domain.JobDefinition{Priority: prio, Tag: jobId}}
	}
	makeTasks := func(num int, jobId string, prio domain.Priority) []*taskState {
		tasks := []*taskState{}
		for i := 0; i < num; i++ {
			def := domain.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}
			tasks = append(tasks, &taskState{JobId: jobId, TaskId: fmt.Sprintf("task%d_P%d", i, prio), Def: def})
		}
		return tasks
	}
	js := []*jobState{
		{
			Job:       makeJob("job1", domain.P0),
			Tasks:     makeTasks(10, "job1", domain.P0),
			Running:   make(map[string]*taskState),
			Completed: make(map[string]*taskState),
		},
		{
			Job:       makeJob("job2", domain.P1),
			Tasks:     makeTasks(10, "job2", domain.P1),
			Running:   make(map[string]*taskState),
			Completed: make(map[string]*taskState),
		},
		{
			Job:       makeJob("job3", domain.P2),
			Tasks:     makeTasks(10, "job3", domain.P2),
			Running:   make(map[string]*taskState),
			Completed: make(map[string]*taskState),
		},
	}
	for _, j := range js {
		nsMap := map[string]*taskState{}
		for _, t := range j.Tasks {
			nsMap[t.TaskId] = t
		}
		j.NotStarted = nsMap
	}

	numNodes := 10
	nodes := []string{}
	for i := 0; i < numNodes; i++ {
		nodes = append(nodes, fmt.Sprintf("node%d", i))
	}
	testCluster := makeTestCluster(nodes...)

	nodeScaleAdjustment := []float32{.05, .2, .75} // Setting this global value explicitly for test consistency.
	config := &OrigSchedulingAlgConfig{
		SoftMaxSchedulableTasks: 50,
		NodeScaleAdjustment:     nodeScaleAdjustment,
	}

	// Check for 7 P2, 2 P1, and 1 P0 tasks
	assignments := getOrigShedAlgTaskAssignments(testCluster, js, config)
	if len(assignments) != numNodes {
		t.Fatalf("Expected %d tasks to be assigned, got %d", numNodes, len(assignments))
	}
	expected := []string{"P2", "P2", "P1", "P0", "P2", "P2", "P2", "P2", "P2", "P1"}
	for i, assignment := range assignments {
		if !strings.HasSuffix(assignment.task.TaskId, expected[i]) {
			t.Fatalf("Idx=%d, expected %s task, got %v", i, expected[i], spew.Sdump(assignment))
		}
	}
}

func Test_StatefulScheduler_NodeScaleFactor(t *testing.T) {
	nodeScaleAdjustment := []float32{.05, .2, .75} // Setting this global value explicitly for test consistency.
	s := &OrigSchedulingAlgConfig{SoftMaxSchedulableTasks: 200, NodeScaleAdjustment: nodeScaleAdjustment}
	numNodes := 21
	numTasks := float32(1)
	if n := ceil(numTasks * s.GetNodeScaleFactor(numNodes, 0)); n != 1 {
		t.Errorf("Expected 1, got %d", n)
	}

	numTasks = float32(100)
	if n := ceil(numTasks * s.GetNodeScaleFactor(numNodes, 0)); n != 1 {
		t.Errorf("Expected 1, got %d", n)
	}
	if n := ceil(numTasks * s.GetNodeScaleFactor(numNodes, 1)); n != 3 {
		t.Errorf("Expected 3, got %d", n)
	}
	if n := ceil(numTasks * s.GetNodeScaleFactor(numNodes, 2)); n != 8 {
		t.Errorf("Expected 8, got %d", n)
	}
}

func getOrigShedAlgTaskAssignments(tc *testCluster, js []*jobState, config *OrigSchedulingAlgConfig) []taskAssignment {

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
	s.SetSchedulingAlg(&OrigSchedulingAlg{Config: config})

	s.config.SchedAlgConfig = nil
	s.inProgressJobs = js
	reqMap := map[string][]*jobState{}
	for _, j := range js {
		if _, ok := reqMap[j.Job.Def.Requestor]; !ok {
			reqMap[j.Job.Def.Requestor] = []*jobState{}
		}
		reqMap[j.Job.Def.Requestor] = append(reqMap[j.Job.Def.Requestor], j)
	}
	s.requestorMap = reqMap

	return s.getTaskAssignments()
}

func Test_StatefulSchedulerRequestorCounts(t *testing.T) {
	origLevel := log.GetLevel()
	log.SetLevel(log.ErrorLevel)
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC()
	s, _, statsRegistry := initializeServices(sc, false)
	s.SetSchedulingAlg(&OrigSchedulingAlg{})

	// create a series of p0 through p2 tasks and run one scheduling iteration
	phase1Profiles := []map[string]string{
		{"numTasks": "5", "requestor": "p0Requestor", "priority": "0"},
		{"numTasks": "3", "requestor": "p1Requestor", "priority": "1"},
		{"numTasks": "1", "requestor": "p2Requestor", "priority": "2"},
	}
	jobIds := addRequestorTestJobsToScheduler(phase1Profiles, s)
	s.step()                          // start the first set of tasks
	time.Sleep(50 * time.Millisecond) // let task threads run to complete
	s.step()                          // get first set of completed tasks and start next set
	// verify that all tasks in job1 and job 3 ran and 1 task from job2 ran and 2 are waiting
	ok := verifyJobStatus("phase1: verify job0 still running", jobIds[0], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.InProgress, domain.InProgress, domain.InProgress}, s, t)
	ok = ok && verifyJobStatus("phase1: verify job1 still running", jobIds[1], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.InProgress}, s, t)
	ok = ok && verifyJobStatus("phase1: verify job2 still running", jobIds[2], domain.Completed,
		[]domain.Status{domain.Completed}, s, t)

	// check the gauges
	ok = ok && checkGauges("p0Requestor", map[string]int{"jobRunning": 1, "jobsWaitingToStart": 0,
		"numRunningTasks": 3, "numWaitingTasks": 0}, s, t, statsRegistry)
	ok = ok && checkGauges("p1Requestor", map[string]int{"jobRunning": 1, "jobsWaitingToStart": 0,
		"numRunningTasks": 1, "numWaitingTasks": 0}, s, t, statsRegistry)
	ok = ok && checkGauges("p2Requestor", map[string]int{"jobRunning": 0, "jobsWaitingToStart": 0,
		"numRunningTasks": 0, "numWaitingTasks": 0}, s, t, statsRegistry)
	if !ok {
		t.Fatal("failed first requestor gauges test")
	}

	phase2Profiles := []map[string]string{
		{"numTasks": "6", "requestor": "p0Requestor", "priority": "0"},
		{"numTasks": "4", "requestor": "p1Requestor", "priority": "1"},
		{"numTasks": "2", "requestor": "p2Requestor", "priority": "2"},
		{"numTasks": "6", "requestor": "p0Requestor", "priority": "0"},
		{"numTasks": "2", "requestor": "p0Requestor", "priority": "0"},
		{"numTasks": "1", "requestor": "p0Requestor", "priority": "0"},
		{"numTasks": "1", "requestor": "p0Requestor", "priority": "0"},
	}

	j := addRequestorTestJobsToScheduler(phase2Profiles, s)
	jobIds = append(jobIds, j[:]...)
	time.Sleep(50 * time.Millisecond) // let prior phase's tasks complete
	s.step()                          // start the next set of tasks
	ok = verifyJobStatus("phase2: verify job3 still running", jobIds[3], domain.InProgress,
		[]domain.Status{domain.InProgress, domain.NotStarted, domain.NotStarted, domain.NotStarted, domain.NotStarted,
			domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase2: verify job4 still running", jobIds[4], domain.InProgress,
		[]domain.Status{domain.InProgress, domain.NotStarted, domain.NotStarted, domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase2: verify job5 still running", jobIds[5], domain.InProgress,
		[]domain.Status{domain.InProgress, domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase2: verify job7 waiting to start", jobIds[6], domain.InProgress,
		[]domain.Status{domain.InProgress, domain.NotStarted, domain.NotStarted, domain.NotStarted, domain.NotStarted,
			domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase2: verify job7 waiting to start", jobIds[7], domain.InProgress,
		[]domain.Status{domain.InProgress, domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase2: verify job8 waiting to start", jobIds[8], domain.InProgress,
		[]domain.Status{domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase2: verify job9 waiting to start", jobIds[9], domain.InProgress,
		[]domain.Status{domain.NotStarted}, s, t)

	// check the gauges
	ok = ok && checkGauges("p0Requestor", map[string]int{"jobRunning": 3, "jobsWaitingToStart": 2,
		"numRunningTasks": 3, "numWaitingTasks": 13}, s, t, statsRegistry)
	ok = ok && checkGauges("p1Requestor", map[string]int{"jobRunning": 1, "jobsWaitingToStart": 0,
		"numRunningTasks": 1, "numWaitingTasks": 3}, s, t, statsRegistry)
	ok = ok && checkGauges("p2Requestor", map[string]int{"jobRunning": 1, "jobsWaitingToStart": 0,
		"numRunningTasks": 1, "numWaitingTasks": 1}, s, t, statsRegistry)
	if !ok {
		t.Fatal("failed first requestor gauges test")
	}

	time.Sleep(50 * time.Millisecond) // let prior phase's tasks' threads complete
	s.step()                          // start the next set of tasks
	ok = verifyJobStatus("phase3: verify job3 still running", jobIds[3], domain.InProgress,
		[]domain.Status{domain.Completed, domain.InProgress, domain.NotStarted, domain.NotStarted, domain.NotStarted,
			domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase3: verify job4 still running", jobIds[4], domain.InProgress,
		[]domain.Status{domain.Completed, domain.InProgress, domain.NotStarted, domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase3: verify job5 still running", jobIds[5], domain.InProgress,
		[]domain.Status{domain.Completed, domain.InProgress}, s, t)
	ok = ok && verifyJobStatus("phase3: verify job7 waiting to start", jobIds[6], domain.InProgress,
		[]domain.Status{domain.Completed, domain.InProgress, domain.NotStarted, domain.NotStarted, domain.NotStarted,
			domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase3: verify job7 waiting to start", jobIds[7], domain.InProgress,
		[]domain.Status{domain.Completed, domain.InProgress}, s, t)
	ok = ok && verifyJobStatus("phase3: verify job8 waiting to start", jobIds[8], domain.InProgress,
		[]domain.Status{domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase3: verify job9 waiting to start", jobIds[9], domain.InProgress,
		[]domain.Status{domain.NotStarted}, s, t)

	// check the gauges
	ok = ok && checkGauges("p0Requestor", map[string]int{"jobRunning": 3, "jobsWaitingToStart": 2,
		"numRunningTasks": 3, "numWaitingTasks": 10}, s, t, statsRegistry)
	ok = ok && checkGauges("p1Requestor", map[string]int{"jobRunning": 1, "jobsWaitingToStart": 0,
		"numRunningTasks": 1, "numWaitingTasks": 2}, s, t, statsRegistry)
	ok = ok && checkGauges("p2Requestor", map[string]int{"jobRunning": 1, "jobsWaitingToStart": 0,
		"numRunningTasks": 1, "numWaitingTasks": 0}, s, t, statsRegistry)
	if !ok {
		t.Fatal("failed first requestor gauges test")
	}

	time.Sleep(50 * time.Millisecond) // let prior phase's tasks' threads complete
	s.step()                          // start the next set of tasks
	ok = verifyJobStatus("phase4: verify job3 still running", jobIds[3], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.InProgress, domain.NotStarted, domain.NotStarted,
			domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase4: verify job4 still running", jobIds[4], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.InProgress, domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase4: verify job5 still running", jobIds[5], domain.Completed,
		[]domain.Status{domain.Completed, domain.Completed}, s, t)
	ok = ok && verifyJobStatus("phase4: verify job6 waiting to start", jobIds[6], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.InProgress, domain.NotStarted, domain.NotStarted,
			domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase4: verify job7 waiting to start", jobIds[7], domain.Completed,
		[]domain.Status{domain.Completed, domain.Completed}, s, t)
	ok = ok && verifyJobStatus("phase4: verify job8 waiting to start", jobIds[8], domain.InProgress,
		[]domain.Status{domain.InProgress}, s, t)
	ok = ok && verifyJobStatus("phase4: verify job9 waiting to start", jobIds[9], domain.InProgress,
		[]domain.Status{domain.InProgress}, s, t)

	// check the gauges
	ok = ok && checkGauges("p0Requestor", map[string]int{"jobRunning": 4, "jobsWaitingToStart": 0,
		"numRunningTasks": 4, "numWaitingTasks": 6}, s, t, statsRegistry)
	ok = ok && checkGauges("p1Requestor", map[string]int{"jobRunning": 1, "jobsWaitingToStart": 0,
		"numRunningTasks": 1, "numWaitingTasks": 1}, s, t, statsRegistry)
	ok = ok && checkGauges("p2Requestor", map[string]int{"jobRunning": 0, "jobsWaitingToStart": 0,
		"numRunningTasks": 0, "numWaitingTasks": 0}, s, t, statsRegistry)
	if !ok {
		t.Fatal("failed first requestor gauges test")
	}

	time.Sleep(50 * time.Millisecond) // let prior phase's tasks' threads complete
	s.step()                          // start the next set of tasks
	ok = verifyJobStatus("phase5: verify job3 still running", jobIds[3], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.Completed, domain.InProgress, domain.InProgress,
			domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase5: verify job4 still running", jobIds[4], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.Completed, domain.InProgress}, s, t)
	ok = ok && verifyJobStatus("phase5: verify job6 waiting to start", jobIds[6], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.Completed, domain.InProgress, domain.InProgress,
			domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase5: verify job8 waiting to start", jobIds[8], domain.Completed,
		[]domain.Status{domain.Completed}, s, t)
	ok = ok && verifyJobStatus("phase5: verify job9 waiting to start", jobIds[9], domain.Completed,
		[]domain.Status{domain.Completed}, s, t)

	// check the gauges
	ok = ok && checkGauges("p0Requestor", map[string]int{"jobRunning": 2, "jobsWaitingToStart": 0,
		"numRunningTasks": 4, "numWaitingTasks": 2}, s, t, statsRegistry)
	ok = ok && checkGauges("p1Requestor", map[string]int{"jobRunning": 1, "jobsWaitingToStart": 0,
		"numRunningTasks": 1, "numWaitingTasks": 0}, s, t, statsRegistry)
	ok = ok && checkGauges("p2Requestor", map[string]int{"jobRunning": 0, "jobsWaitingToStart": 0,
		"numRunningTasks": 0, "numWaitingTasks": 0}, s, t, statsRegistry)
	if !ok {
		t.Fatal("failed first requestor gauges test")
	}

	time.Sleep(50 * time.Millisecond) // let prior phase's tasks' threads complete
	s.step()                          // start the next set of tasks
	ok = verifyJobStatus("phase6: verify job3 still running", jobIds[3], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.Completed, domain.Completed, domain.Completed,
			domain.InProgress}, s, t)
	ok = ok && verifyJobStatus("phase6: verify job4 still running", jobIds[4], domain.Completed,
		[]domain.Status{domain.Completed, domain.Completed, domain.Completed, domain.Completed}, s, t)
	ok = ok && verifyJobStatus("phase6: verify job6 waiting to start", jobIds[6], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.Completed, domain.Completed, domain.Completed,
			domain.InProgress}, s, t)

	// check the gauges
	ok = ok && checkGauges("p0Requestor", map[string]int{"jobRunning": 2, "jobsWaitingToStart": 0,
		"numRunningTasks": 2, "numWaitingTasks": 0}, s, t, statsRegistry)
	ok = ok && checkGauges("p1Requestor", map[string]int{"jobRunning": 0, "jobsWaitingToStart": 0,
		"numRunningTasks": 0, "numWaitingTasks": 0}, s, t, statsRegistry)
	ok = ok && checkGauges("p2Requestor", map[string]int{"jobRunning": 0, "jobsWaitingToStart": 0,
		"numRunningTasks": 0, "numWaitingTasks": 0}, s, t, statsRegistry)
	if !ok {
		t.Fatal("failed first requestor gauges test")
	}

	time.Sleep(50 * time.Millisecond) // let prior phase's tasks' threads complete
	s.step()                          // start the next set of tasks
	ok = verifyJobStatus("phase7: verify job3 still running", jobIds[3], domain.Completed,
		[]domain.Status{domain.Completed, domain.Completed, domain.Completed, domain.Completed, domain.Completed,
			domain.Completed}, s, t)
	ok = ok && verifyJobStatus("phase7: verify job6 waiting to start", jobIds[6], domain.Completed,
		[]domain.Status{domain.Completed, domain.Completed, domain.Completed, domain.Completed, domain.Completed,
			domain.Completed}, s, t)

	// check the gauges
	ok = ok && checkGauges("p0Requestor", map[string]int{"jobRunning": 0, "jobsWaitingToStart": 0,
		"numRunningTasks": 0, "numWaitingTasks": 0}, s, t, statsRegistry)
	ok = ok && checkGauges("p1Requestor", map[string]int{"jobRunning": 0, "jobsWaitingToStart": 0,
		"numRunningTasks": 0, "numWaitingTasks": 0}, s, t, statsRegistry)
	ok = ok && checkGauges("p2Requestor", map[string]int{"jobRunning": 0, "jobsWaitingToStart": 0,
		"numRunningTasks": 0, "numWaitingTasks": 0}, s, t, statsRegistry)
	if !ok {
		t.Fatal("failed first requestor gauges test")
	}

	if len(s.requestorsCounts) > 0 {
		t.Fatal("scheduler's requestor count map was not emptied!")
	}

	log.SetLevel(origLevel)
}

func addRequestorTestJobsToScheduler(jobProfiles []map[string]string, s *statefulScheduler) []string {
	jobIds := []string{}
	for _, jobProfile := range jobProfiles {
		numTasks, _ := strconv.Atoi(jobProfile["numTasks"])
		priority, _ := strconv.Atoi(jobProfile["priority"])
		jobId, _, _ := putJobInScheduler(numTasks, s, "complete 0", jobProfile["requestor"],
			domain.Priority(priority))
		s.addJobs()
		jobIds = append(jobIds, jobId)
	}

	return jobIds
}
