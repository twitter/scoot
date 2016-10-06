// +build property_test

package scheduler

import (
	"fmt"
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/sched"
	"testing"
)

// verify that jobs are distributed evenly
func Test_StatefulScheduler_TasksDistributedEvenly(t *testing.T) {
	jobDef := sched.GenJobDef(1000)
	s := makeDefaultStatefulScheduler()

	//initialize NodeMap to keep track of tasks per node
	taskMap := make(map[string]cluster.NodeId)

	/*jobId, _ :=*/ s.ScheduleJob(jobDef)
	s.step()

	for len(s.inProgressJobs) > 0 {
		s.step()

		for nodeId, state := range s.clusterState.nodes {
			if state.runningTask != noTask {
				taskMap[state.runningTask] = nodeId
			}
		}
	}

	taskCountMap := make(map[cluster.NodeId]int)
	for _, nodeId := range taskMap {
		taskCountMap[nodeId]++
	}

	// The in memory workers aren't doing anything interesting except sleeping distribution
	// should be even with in 180 - 220 nodes otherwise something is wrong.
	for nodeId, taskCount := range taskCountMap {
		if taskCount < 180 || taskCount > 220 {
			t.Fatalf(`Tasks were not evenly distributed across nodes.  Expected each node
				to have 190 to 210 tasks executed on it. %v had an unequal number of tasks %v scheduled 
				on it.  TaskCountMap: %+v`, nodeId, taskCount, taskCountMap)
		}
	}

	fmt.Printf("Task to Node Distribution: %+v", taskCountMap)
}
