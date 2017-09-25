package scheduler

import (
	"fmt"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/luci/go-render/render"
	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/saga/sagalogs"
	"github.com/twitter/scoot/sched"
	"github.com/twitter/scoot/tests/testhelpers"
)

func Test_TaskAssignment_NoNodesAvailable(t *testing.T) {
	job := sched.GenJob(testhelpers.GenJobId(testhelpers.NewRand()), 10)
	jobAsBytes, _ := job.Serialize()

	saga, _ := sagalogs.MakeInMemorySagaCoordinator().MakeSaga(job.Id, jobAsBytes)
	js := newJobState(&job, saga, nil)

	// create a test cluster with no nodes
	testCluster := makeTestCluster()
	cs := newClusterState(testCluster.nodes, testCluster.ch, nil, stats.NilStatsReceiver())
	assignments, _ := getTaskAssignments(cs, []*jobState{js}, nil, nil, nil)

	if len(assignments) != 0 {
		t.Errorf("Assignments on a cluster with no nodes should not return any assignments")
	}
}

func Test_TaskAssignment_NoTasks(t *testing.T) {
	// create a test cluster with no nodes
	testCluster := makeTestCluster("node1", "node2", "node3", "node4", "node5")
	cs := newClusterState(testCluster.nodes, testCluster.ch, nil, stats.NilStatsReceiver())
	assignments, _ := getTaskAssignments(cs, []*jobState{}, nil, nil, nil)

	if len(assignments) != 0 {
		t.Errorf("Assignments on a cluster with no nodes should not return any assignments")
	}
}

// Currently we schedule based on availability only.  This
// Test verifies that tasks are scheduled on all available nodes.
func Test_TaskAssignments_TasksScheduled(t *testing.T) {
	job := sched.GenJob(testhelpers.GenJobId(testhelpers.NewRand()), 10)
	jobAsBytes, _ := job.Serialize()

	saga, _ := sagalogs.MakeInMemorySagaCoordinator().MakeSaga(job.Id, jobAsBytes)
	js := newJobState(&job, saga, nil)
	req := map[string][]*jobState{"": []*jobState{js}}

	// create a test cluster with no nodes
	testCluster := makeTestCluster("node1", "node2", "node3", "node4", "node5")
	cs := newClusterState(testCluster.nodes, testCluster.ch, nil, stats.NilStatsReceiver())
	unScheduledTasks := js.getUnScheduledTasks()
	assignments, _ := getTaskAssignments(cs, []*jobState{js}, req, nil, stats.NilStatsReceiver())

	if len(assignments) != min(len(unScheduledTasks), len(testCluster.nodes)) {
		t.Errorf(`Expected as many tasks as possible to be scheduled: NumScheduled %v, 
      Number Of Available Nodes %v, Number of Unscheduled Tasks %v`,
			len(assignments),
			len(testCluster.nodes),
			len(unScheduledTasks))
	}
}

func Test_TaskAssignment_Affinity(t *testing.T) {
	testCluster := makeTestCluster("node1", "node2", "node3")
	cs := newClusterState(testCluster.nodes, testCluster.ch, nil, stats.NilStatsReceiver())
	tasks := []*taskState{
		&taskState{TaskId: "task1", Def: sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
		&taskState{TaskId: "task2", Def: sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
		&taskState{TaskId: "task3", Def: sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapB"}}},
		&taskState{TaskId: "task4", Def: sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
		&taskState{TaskId: "task5", Def: sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapB"}}},
	}
	js := &jobState{Job: &sched.Job{}, Tasks: tasks}
	req := map[string][]*jobState{"": []*jobState{js}}
	assignments, _ := getTaskAssignments(cs, []*jobState{js}, req, nil, nil)
	if len(assignments) != 3 {
		t.Errorf("Expected first three tasks to be assigned, got %v", len(assignments))
	}

	// Schedule the first three tasks and complete task2, task3.
	taskNodes := map[string]cluster.NodeId{}
	for _, as := range assignments {
		taskNodes[as.task.TaskId] = as.nodeSt.node.Id()
		cs.taskScheduled(as.nodeSt.node.Id(), "job1", as.task.TaskId, as.task.Def.SnapshotID)
		js.taskStarted(as.task.TaskId, &taskRunner{})
		if as.task.TaskId != "task1" {
			cs.taskCompleted(as.nodeSt.node.Id(), false)
			js.taskCompleted(as.task.TaskId, true)
		}
	}

	// Add a new idle node and then confirm that task4, task5 are assigned based on affinity.
	cs.update([]cluster.NodeUpdate{
		cluster.NodeUpdate{UpdateType: cluster.NodeAdded, Id: "node4", Node: cluster.NewIdNode("node4")},
	})
	assignments, _ = getTaskAssignments(cs, []*jobState{js}, req, nil, nil)
	for _, as := range assignments {
		if as.task.TaskId == "task4" {
			if as.nodeSt.node.Id() != taskNodes["task2"] {
				t.Errorf("Expected task4 to take over task2's node: %v", render.Render(as))
			}
		} else {
			if as.nodeSt.node.Id() != taskNodes["task3"] {
				t.Errorf("Expected task5 to take over task3's node: %v", render.Render(as))
			}
		}
	}
}

// We want to see three tasks with TagX scheduled first, followed by one TagY, then the final TagX
func Test_TaskAssignments_RequestorBatching(t *testing.T) {
	js := []*jobState{
		&jobState{
			Job: &sched.Job{
				Id:  "job1",
				Def: sched.JobDefinition{Tag: "TagX"},
			},
			Tasks: []*taskState{
				&taskState{JobId: "job1", TaskId: "task1", Def: sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
				&taskState{JobId: "job1", TaskId: "task2", Def: sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
			},
		},
		&jobState{
			Job: &sched.Job{
				Id:  "job2",
				Def: sched.JobDefinition{Tag: "TagY"},
			},
			Tasks: []*taskState{
				&taskState{JobId: "job2", TaskId: "task1", Def: sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
			},
		},
		&jobState{
			Job: &sched.Job{
				Id:  "job3",
				Def: sched.JobDefinition{Tag: "TagX"},
			},
			Tasks: []*taskState{
				&taskState{JobId: "job3", TaskId: "task1", Def: sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
				&taskState{JobId: "job3", TaskId: "task2", Def: sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
			},
		},
	}

	nodes := []string{}
	for i := 0; i < 6; i++ {
		nodes = append(nodes, fmt.Sprintf("node%d", i))
	}
	testCluster := makeTestCluster(nodes...)
	cs := newClusterState(testCluster.nodes, testCluster.ch, nil, stats.NilStatsReceiver())

	req := map[string][]*jobState{"": js}
	config := &SchedulerConfig{
		SoftMaxSchedulableTasks: 10, // We want numTasks*GetNodeScaleFactor()==3 to define a specific order for scheduling.
	}

	assignments, _ := getTaskAssignments(cs, js, req, config, nil)
	if len(assignments) != 5 {
		t.Errorf("Expected all five tasks to be assigned, got %v", len(assignments))
	}
	if assignments[0].task.JobId != "job1" || assignments[0].task.TaskId != "task1" {
		t.Errorf("Expected 0:job1.task1, got: %v", spew.Sdump(assignments[0]))
	}
	if assignments[1].task.JobId != "job1" || assignments[1].task.TaskId != "task2" {
		t.Errorf("Expected 1:job1.task2, got: %v", spew.Sdump(assignments[1]))
	}
	if assignments[2].task.JobId != "job3" || assignments[2].task.TaskId != "task1" {
		t.Errorf("Expected 2:job3.task1, got: %v", spew.Sdump(assignments[2]))
	}
	if assignments[3].task.JobId != "job2" || assignments[3].task.TaskId != "task1" {
		t.Errorf("Expected 3:job2.task1, got: %v", spew.Sdump(assignments[3]))
	}
	if assignments[4].task.JobId != "job3" || assignments[4].task.TaskId != "task2" {
		t.Errorf("Expected 4:job3.task2, got: %v", spew.Sdump(assignments[4]))
	}
}

/*
Add Job1.P0, Job2.P1 Job3.P2, Job4.P3, Job5.P0
With 4 nodes, expect: scheduled Job4, Job3, Job2, Job1
*/
func Test_TaskAssignments_PrioritySimple(t *testing.T) {
	makeJob := func(jobId string, prio sched.Priority) *sched.Job {
		return &sched.Job{Id: jobId, Def: sched.JobDefinition{Priority: prio, Tag: jobId}}
	}
	makeTasks := func(jobId string) []*taskState {
		return []*taskState{
			&taskState{JobId: jobId, TaskId: "task1", Def: sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
		}
	}
	js := []*jobState{
		&jobState{
			Job:   makeJob("job1", sched.P0),
			Tasks: makeTasks("job1"),
		},
		&jobState{
			Job:   makeJob("job2", sched.P1),
			Tasks: makeTasks("job2"),
		},
		&jobState{
			Job:   makeJob("job3", sched.P2),
			Tasks: makeTasks("job3"),
		},
		&jobState{
			Job:   makeJob("job4", sched.P3),
			Tasks: makeTasks("job4"),
		},
		&jobState{
			Job:   makeJob("job5", sched.P0),
			Tasks: makeTasks("job5"),
		},
	}

	nodes := []string{}
	for i := 0; i < 4; i++ {
		nodes = append(nodes, fmt.Sprintf("node%d", i))
	}
	testCluster := makeTestCluster(nodes...)
	cs := newClusterState(testCluster.nodes, testCluster.ch, nil, stats.NilStatsReceiver())

	req := map[string][]*jobState{"": js}

	assignments, _ := getTaskAssignments(cs, js, req, nil, nil)
	if len(assignments) != 4 {
		t.Errorf("Expected four tasks to be assigned, got %v", len(assignments))
	}
	if assignments[0].task.JobId != "job4" {
		t.Errorf("Expected 0:job4: %v", spew.Sdump(assignments[0]))
	}
	if assignments[1].task.JobId != "job3" {
		t.Errorf("Expected 1:job3, got: %v", spew.Sdump(assignments[1]))
	}
	if assignments[2].task.JobId != "job2" {
		t.Errorf("Expected 2:job2, got: %v", spew.Sdump(assignments[2]))
	}
	if assignments[3].task.JobId != "job1" {
		t.Errorf("Expected 3:job1, got: %v", spew.Sdump(assignments[3]))
	}
}

/*
Set NodeScaleFactor=.2 (10 NumConfiguredNodes / 50 SoftMaxSchedulableTasks) to get the following scheduling.
Add jobs: (10 P3 Tasks), (10 P2 Tasks), (10 P1 Tasks), (10 P0 Tasks)
With 10 nodes: assign all 10 to the p3 tasks.
After finishing p3 tasks: assign nodes for 5 P2, 3 P1, and 2 P0 tasks
*/
func Test_TaskAssignments_PriorityStages(t *testing.T) {
	makeJob := func(jobId string, prio sched.Priority) *sched.Job {
		return &sched.Job{Id: jobId, Def: sched.JobDefinition{Priority: prio, Tag: jobId}}
	}
	makeTasks := func(num int, jobId string, prio sched.Priority) []*taskState {
		tasks := []*taskState{}
		for i := 0; i < num; i++ {
			def := sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}
			tasks = append(tasks, &taskState{JobId: jobId, TaskId: fmt.Sprintf("task%d_P%d", i, prio), Def: def})
		}
		return tasks
	}
	js := []*jobState{
		&jobState{
			Job:   makeJob("job1", sched.P0),
			Tasks: makeTasks(10, "job1", sched.P0),
		},
		&jobState{
			Job:   makeJob("job2", sched.P1),
			Tasks: makeTasks(10, "job2", sched.P1),
		},
		&jobState{
			Job:   makeJob("job3", sched.P2),
			Tasks: makeTasks(10, "job3", sched.P2),
		},
		&jobState{
			Job:   makeJob("job4", sched.P3),
			Tasks: makeTasks(10, "job4", sched.P3),
		},
	}

	nodes := []string{}
	for i := 0; i < 10; i++ {
		nodes = append(nodes, fmt.Sprintf("node%d", i))
	}
	testCluster := makeTestCluster(nodes...)
	cs := newClusterState(testCluster.nodes, testCluster.ch, nil, stats.NilStatsReceiver())

	req := map[string][]*jobState{"": js}
	config := &SchedulerConfig{
		SoftMaxSchedulableTasks: 50, // We want numTasks*GetNodeScaleFactor()==2 to define a specific order for scheduling.
	}

	// Check for all 10 P3 tasks
	assignments, _ := getTaskAssignments(cs, js, req, config, nil)
	if len(assignments) != 10 {
		t.Fatalf("Expected ten tasks to be assigned, got %v", len(assignments))
	}
	for _, assignment := range assignments {
		if !strings.HasSuffix(assignment.task.TaskId, "_P3") {
			t.Fatalf("Expected all P3 tasks, got %v", spew.Sdump(assignment))
		}
		for _, j := range js {
			if j.Job.Id == assignment.task.JobId {
				j.taskCompleted(assignment.task.TaskId, true)
			}
		}
	}

	// Check for 5 P2, 3 P1, and 2 P0 tasks
	NodeScaleAdjustment = 0 //Reset this global setting to simplify testing here.
	assignments, _ = getTaskAssignments(cs, js, req, config, nil)
	if len(assignments) != 10 {
		t.Fatalf("Expected ten tasks to be assigned, got %v", len(assignments))
	}
	expected := []string{"P2", "P2", "P1", "P1", "P0", "P0", "P2", "P2", "P2", "P1"}
	for i, assignment := range assignments {
		if !strings.HasSuffix(assignment.task.TaskId, expected[i]) {
			t.Fatalf("Idx=%d, expected %s task, got %v", i, expected[i], spew.Sdump(assignment))
		}
	}

}

/*
TODO: _PriorityKill
Add jobs P2a, P1, P0, P2b, P3, P3
3 nodes -> kill P0, P1, P2b in order
*/
