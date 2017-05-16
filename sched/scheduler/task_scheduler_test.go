package scheduler

import (
	"testing"

	"github.com/luci/go-render/render"
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/saga/sagalogs"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/tests/testhelpers"
)

func Test_TaskAssignment_NoNodesAvailable(t *testing.T) {
	job := sched.GenJob(testhelpers.GenJobId(testhelpers.NewRand()), 10)
	jobAsBytes, _ := job.Serialize()

	saga, _ := sagalogs.MakeInMemorySagaCoordinator().MakeSaga(job.Id, jobAsBytes)
	js := newJobState(&job, saga)

	// create a test cluster with no nodes
	testCluster := makeTestCluster()
	cs := newClusterState(testCluster.nodes, testCluster.ch, nil)
	assignments, _ := getTaskAssignments(cs, []*jobState{js}, nil)

	if len(assignments) != 0 {
		t.Errorf("Assignments on a cluster with no nodes should not return any assignments")
	}
}

func Test_TaskAssignment_NoTasks(t *testing.T) {
	// create a test cluster with no nodes
	testCluster := makeTestCluster("node1", "node2", "node3", "node4", "node5")
	cs := newClusterState(testCluster.nodes, testCluster.ch, nil)
	assignments, _ := getTaskAssignments(cs, []*jobState{}, nil)

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
	js := newJobState(&job, saga)
	req := map[string][]*jobState{"": []*jobState{js}}

	// create a test cluster with no nodes
	testCluster := makeTestCluster("node1", "node2", "node3", "node4", "node5")
	cs := newClusterState(testCluster.nodes, testCluster.ch, nil)
	unScheduledTasks := js.getUnScheduledTasks()
	assignments, _ := getTaskAssignments(cs, []*jobState{js}, req)

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
	cs := newClusterState(testCluster.nodes, testCluster.ch, nil)
	tasks := []*taskState{
		&taskState{TaskId: "task1", Def: sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
		&taskState{TaskId: "task2", Def: sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
		&taskState{TaskId: "task3", Def: sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapB"}}},
		&taskState{TaskId: "task4", Def: sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapA"}}},
		&taskState{TaskId: "task5", Def: sched.TaskDefinition{Command: runner.Command{SnapshotID: "snapB"}}},
	}
	js := &jobState{Job: &sched.Job{}, Tasks: tasks}
	req := map[string][]*jobState{"": []*jobState{js}}
	assignments, _ := getTaskAssignments(cs, []*jobState{js}, req)
	if len(assignments) != 3 {
		t.Errorf("Expected first three tasks to be assigned, got %v", len(assignments))
	}

	// Schedule the first three tasks and complete task2, task3.
	taskNodes := map[string]cluster.NodeId{}
	for _, as := range assignments {
		taskNodes[as.task.TaskId] = as.node.Id()
		cs.taskScheduled(as.node.Id(), as.task.TaskId, as.task.Def.SnapshotID)
		js.taskStarted(as.task.TaskId, &taskRunner{})
		if as.task.TaskId != "task1" {
			cs.taskCompleted(as.node.Id(), as.task.TaskId, false)
			js.taskCompleted(as.task.TaskId)
		}
	}

	// Add a new idle node and then confirm that task4, task5 are assigned based on affinity.
	cs.update([]cluster.NodeUpdate{
		cluster.NodeUpdate{UpdateType: cluster.NodeAdded, Id: "node4", Node: cluster.NewIdNode("node4")},
	})
	assignments, _ = getTaskAssignments(cs, []*jobState{js}, req)
	for _, as := range assignments {
		if as.task.TaskId == "task4" {
			if as.node.Id() != taskNodes["task2"] {
				t.Errorf("Expected task4 to take over task2's node: %v", render.Render(as))
			}
		} else {
			if as.node.Id() != taskNodes["task3"] {
				t.Errorf("Expected task5 to take over task3's node: %v", render.Render(as))
			}
		}
	}
}
