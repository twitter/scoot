package scheduler

import (
	"github.com/scootdev/scoot/cloud/cluster"
)

type taskAssignments struct {
	node cluster.Node
	task *taskState
}

// Returns a list of taskAssigments of task to available node.
// Also returns a modified copy of clusterState.Affinity for the caller to apply (so this remains a pure fn).
// Does best effort scheduling which tries to assign tasks to nodes already primed for similar tasks.
// Not all tasks are guaranteed to be scheduled.
func getTaskAssignments(cs *clusterState, tasks []*taskState) ([]taskAssignments, map[string]*nodeGroups) {
	var assignments []taskAssignments
	affinity := map[string]*nodeGroups{}
	snapshotIds := []string{}

	// Create a copy of affinity to modify based on new scheduling.
	for snapId, groups := range cs.affinity {
		affinity[snapId] = newNodeGroups()
		for nodeId, node := range groups.idle {
			affinity[snapId].idle[nodeId] = node
		}
		for nodeId, node := range groups.busy {
			affinity[snapId].busy[nodeId] = node
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
			if groups, ok := affinity[snapId]; ok {
				for nodeId, node := range groups.idle {
					assignments = append(assignments, taskAssignments{node: node, task: task})
					if _, ok := affinity[task.Def.SnapshotID]; !ok {
						affinity[task.Def.SnapshotID] = newNodeGroups()
					}
					affinity[task.Def.SnapshotID].busy[nodeId] = node
					delete(affinity[snapId].idle, nodeId)
					continue Loop
				}
			}
		}
		break // No more idle nodes.
	}

	return assignments, affinity
}
