package scheduler

import (
	"github.com/scootdev/scoot/cloud/cluster"
)

type taskAssignments struct {
	node cluster.Node
	task *taskState
}

// Returns a list of taskAssigments of task to available node.
// Also returns a modified copy of clusterState.nodeGroups for the caller to apply (so this remains a pure fn).
// Does best effort scheduling which tries to assign tasks to nodes already primed for similar tasks.
// Not all tasks are guaranteed to be scheduled.
func getTaskAssignments(cs *clusterState, tasks []*taskState) ([]taskAssignments, map[string]*nodeGroup) {
	var assignments []taskAssignments
	nodeGroups := map[string]*nodeGroup{}
	snapshotIds := []string{}

	// Create a copy of nodeGroups to modify based on new scheduling.
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

	// Loop over all snapshotIDs looking for an idle node. Prefer, in order:
	// - Hot node for the given snapshotID (one whose last task shared the same snapshotID).
	// - New untouched node (or node whose last task used an empty snapshotID)
	// - Any node from the idle pools of nodes associated with other snapshotIDs.
Loop:
	for _, task := range tasks {
		snapIds := append([]string{task.Def.SnapshotID, ""}, snapshotIds...)
		for _, snapId := range snapIds {
			if groups, ok := nodeGroups[snapId]; ok {
				for nodeId, node := range groups.idle {
					assignments = append(assignments, taskAssignments{node: node, task: task})
					if _, ok := nodeGroups[task.Def.SnapshotID]; !ok {
						nodeGroups[task.Def.SnapshotID] = newNodeGroup()
					}
					nodeGroups[task.Def.SnapshotID].busy[nodeId] = node
					delete(nodeGroups[snapId].idle, nodeId)
					continue Loop
				}
			}
		}
		break // No more idle nodes.
	}

	return assignments, nodeGroups
}
