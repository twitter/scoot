package scheduler

import (
	"github.com/scootdev/scoot/cloud/cluster"
)

type taskAssignments struct {
	node cluster.Node
	task *taskState
}

// Returns a list of taskAssigments of task to available node.  Not all
// tasks are guaranteed to be scheduled.  Does best effort scheduling
func GetTasksAssignments(cs *clusterState, tasks []*taskState) []taskAssignments {

	// for now just assign any task to any available node
	var tas []taskAssignments
	taskIndex := 0

	for _, nodeState := range cs.nodes {
		if nodeState.runningTask == noTask {
			if taskIndex < len(tasks) {
				tas = append(tas, taskAssignments{
					node: nodeState.node,
					task: tasks[taskIndex],
				})
				taskIndex++
			} else {
				// we've scheduled all tasks
				break
			}
		}
	}
	return tas
}
