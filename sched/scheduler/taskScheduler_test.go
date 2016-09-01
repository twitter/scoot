package scheduler

import (
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/tests/testhelpers"
	"math"
	"testing"
)

func Test_TaskAssignment_NoNodesAvailable(t *testing.T) {
	job := sched.GenJob(testhelpers.GenJobId(testhelpers.NewRand()), 10)
	jobState := NewJobState(job, nil)

	// create a test cluster with no nodes
	testCluster := makeTestCluster()
	cs := newClusterState(testCluster.nodes, testCluster.ch)
	assignments := GetTasksAssignments(cs, jobState.GetUnScheduledTasks())

	if len(assignments) != 0 {
		t.Errorf("Assignments on a cluster with no nodes should not return any assignments")
	}
}

func Test_TaskAssignment_NoTasks(t *testing.T) {
	// create a test cluster with no nodes
	testCluster := makeTestCluster("node1", "node2", "node3", "node4", "node5")
	cs := newClusterState(testCluster.nodes, testCluster.ch)
	assignments := GetTasksAssignments(cs, []*taskState{})

	if len(assignments) != 0 {
		t.Errorf("Assignments on a cluster with no nodes should not return any assignments")
	}
}

// Currently we schedule based on availability only.  This
// Test verifies that tasks are scheduled on all available nodes.
func Test_TaskAssignments_TasksScheduled(t *testing.T) {
	job := sched.GenJob(testhelpers.GenJobId(testhelpers.NewRand()), 10)
	jobState := NewJobState(job, nil)

	// create a test cluster with no nodes
	testCluster := makeTestCluster("node1", "node2", "node3", "node4", "node5")
	cs := newClusterState(testCluster.nodes, testCluster.ch)
	unScheduledTasks := jobState.GetUnScheduledTasks()
	assignments := GetTasksAssignments(cs, unScheduledTasks)

	if float64(len(assignments)) != math.Min(float64(len(unScheduledTasks)), float64(len(testCluster.nodes))) {
		t.Errorf(`Expected as many tasks as possible to be scheduled: NumScheduled %v, 
      Number Of Available Nodes %v, Number of Unscheduled Tasks %v`,
			len(assignments),
			len(testCluster.nodes),
			len(unScheduledTasks))
	}
}
