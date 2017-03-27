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
func getTaskAssignments(cs *clusterState, tasks []*taskState) ([]taskAssignments, map[string]*nodeSet) {
	var tas []taskAssignments
	affinity := map[string]*nodeSet{}
	snapshotIds := []string{}

	// Create a copy of affinity to modify based on new scheduling.
	for snapId, set := range cs.affinity {
		affinity[snapId] = newNodeSet()
		for nodeId, node := range set.idle {
			affinity[snapId].idle[nodeId] = node
		}
		for nodeId, node := range set.busy {
			affinity[snapId].busy[nodeId] = node
		}
		snapshotIds = append(snapshotIds, snapId)
	}

	// Loop over all snapshotIDs looking for an idle node. Prefer, in order:
	// - Hot node for the given snapshotID
	// - New untouched node (or node run with an empty snapshotID)
	// - Any node that's hot for a different snapshotID.
	// Note: tasks are expected to have a valid snapshotID but if it's empty this should still work.
Loop:
	for _, task := range tasks {
		snapIds := append([]string{task.Def.SnapshotID, ""}, snapshotIds...)
		for _, snapId := range snapIds {
			if set, ok := affinity[snapId]; ok {
				for nodeId, node := range set.idle {
					tas = append(tas, taskAssignments{node: node, task: task})
					if _, ok := affinity[task.Def.SnapshotID]; !ok {
						affinity[task.Def.SnapshotID] = newNodeSet()
					}
					affinity[task.Def.SnapshotID].busy[nodeId] = node
					delete(affinity[snapId].idle, nodeId)
					continue Loop
				}
			}
		}
		break // No more idle nodes.
	}

	return tas, affinity
}
