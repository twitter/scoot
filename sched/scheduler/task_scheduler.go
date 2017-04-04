package scheduler

import (
	log "github.com/Sirupsen/logrus"

	"github.com/scootdev/scoot/cloud/cluster"
)

type taskAssignment struct {
	node cluster.Node
	task *taskState
}

// Returns a list of taskAssigments of task to available node.
// Also returns a modified copy of clusterState.nodeGroups for the caller to apply (so this remains a pure fn).
// Note: pure fn because it's confusing to have getTaskAssignments() modify clusterState based on the proposed
//       scheduling and also require that the caller apply final modifications to clusterState as a second step)
//
// Does best effort scheduling which tries to assign tasks to nodes already primed for similar tasks.
// Not all tasks are guaranteed to be scheduled.
func getTaskAssignments(cs *clusterState, tasks []*taskState) ([]taskAssignment, map[string]*nodeGroup) {
	// Create a copy of cs.nodeGroups to modify based on new scheduling.
	snapshotIds := []string{}
	nodeGroups := map[string]*nodeGroup{}
	for snapId, groups := range cs.nodeGroups {
		nodeGroups[snapId] = newNodeGroup()
		for nodeId, node := range groups.idle {
			nodeGroups[snapId].idle[nodeId] = node
		}
		for nodeId, node := range groups.busy {
			nodeGroups[snapId].busy[nodeId] = node
		}
		snapshotIds = append(snapshotIds, snapId)
	}

	// Loop over all snapshotIds looking for an idle node. Prefer, in order:
	// - Hot node for the given snapshotId (one whose last task shared the same snapshotId).
	// - New untouched node (or node whose last task used an empty snapshotId)
	// - A random node from the idle pools of nodes associated with other snapshotIds.
	var assignments []taskAssignment
	remainingTasks := assign(cs, tasks, &assignments, nodeGroups, []string{""})
	remainingTasks = assign(cs, remainingTasks, &assignments, nodeGroups, snapshotIds)
	if len(remainingTasks) == 0 {
		log.Infof("Scheduled all tasks (%d)", len(tasks))
	} else {
		log.Infof("Unable to schedule all tasks, remaining=%d/%d", len(remainingTasks), len(tasks))
	}
	return assignments, nodeGroups
}

// Helper fn, appends to 'assignments' and updates nodeGroups.
// Returns tasks that couldn't be scheduled using the task's snapshotId or any of this in snapIds.
//TODO(jschiller): soft/hard # of nodes reserved.
//TODO(jschiller): take idle nodes from LRU snapId and/or from snapIds with spare soft reservations.
func assign(
	cs *clusterState,
	tasks []*taskState,
	assignments *[]taskAssignment,
	nodeGroups map[string]*nodeGroup,
	snapIds []string,
) []*taskState {

	var remaining []*taskState
	numTotalTasks := len(*assignments) + len(tasks)
Loop:
	for _, task := range tasks {
		for _, snapId := range append([]string{task.Def.SnapshotID}, snapIds...) {
			if groups, ok := nodeGroups[snapId]; ok {
				for nodeId, ns := range groups.idle {
					if ns.Lost() || ns.Flaky() {
						continue
					}
					*assignments = append(*assignments, taskAssignment{node: ns.node, task: task})
					if _, ok := nodeGroups[task.Def.SnapshotID]; !ok {
						nodeGroups[task.Def.SnapshotID] = newNodeGroup()
					}
					nodeGroups[task.Def.SnapshotID].busy[nodeId] = ns
					delete(nodeGroups[snapId].idle, nodeId)
					log.Infof("Scheduled jobId=%s, taskId=%s, node=%s, progress=%d/%d",
						task.JobId, task.TaskId, nodeId, len(*assignments), numTotalTasks)
					continue Loop
				}
			}
		}
		remaining = append(remaining, task)
	}
	return remaining
}
