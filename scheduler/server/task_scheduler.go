package server

import (
	"math"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common/stats"
)

type taskAssignment struct {
	nodeSt *nodeState
	task   *taskState
}

// Clients will check for this string to differentiate between scoot and user initiated actions.
const RebalanceRequestedErrStr = "RebalanceRequested"

// Returns a list of taskAssigments of task to free node.
// Also returns a modified copy of clusterState.nodeGroups for the caller to apply (so this remains a pure fn).
// Note: pure fn because it's confusing to have getTaskAssignments() modify clusterState based on the proposed
//       scheduling and also require that the caller apply final modifications to clusterState as a second step)
//
// Does best effort scheduling which tries to assign tasks to nodes already primed for similar tasks.
// Not all tasks are guaranteed to be scheduled.
func (s *statefulScheduler) getTaskAssignments() []taskAssignment {
	defer s.stat.Latency(stats.SchedTaskAssignmentsLatency_ms).Time().Stop()

	// Exit if there are no unscheduled tasks.
	waitingTasksFound := false
	for _, j := range s.inProgressJobs {
		if len(j.NotStarted) > 0 {
			waitingTasksFound = true
			break
		}
	}
	if !waitingTasksFound {
		return nil
	}

	tasks, stopTasks := s.config.SchedAlg.GetTasksToBeAssigned(s.inProgressJobs, s.stat, s.clusterState, s.requestorMap)
	// Exit if no tasks qualify to be scheduled.
	if len(tasks) == 0 {
		if len(stopTasks) != 0 {
			log.Errorf("task assignment returned tasks to stop but none to start.  Ignoring the (%d len) stopTasks list", len(stopTasks))
		}
		return nil
	}
	log.WithFields(log.Fields{"numStartingTasks": len(tasks), "numStoppingTasks": len(stopTasks)}).Info("scheduling returned")

	// stop the tasks in stopTasks (we are rebalancing the workers)
	for _, task := range stopTasks {
		jobState := s.getJob(task.JobId)
		logFields := log.Fields{
			"jobID":     task.JobId,
			"requestor": jobState.Job.Def.Requestor,
			"jobType":   jobState.Job.Def.JobType,
			"tag":       jobState.Job.Def.Tag,
		}
		msgs := s.abortTask(jobState, task, logFields, RebalanceRequestedErrStr)
		if len(msgs) > 0 {
			if err := jobState.Saga.BulkMessage(msgs); err != nil {
				logFields["err"] = err
				log.WithFields(logFields).Error("abortTask saga.BulkMessage failure")
			}
		}
	}

	// Loop over all cluster snapshotIds looking for a usable node. Prefer, in order:
	// - Hot node for the given snapshotId (one whose last task shared the same snapshotId).
	// - New untouched node (or node whose last task used an empty snapshotId)
	// - A random free node from the idle pools of nodes associated with other snapshotIds.
	assignments := s.assign(tasks)
	log.WithFields(
		log.Fields{
			"numAssignments": len(assignments),
			"numTasks":       len(tasks),
			"tag":            tasks[0].Def.Tag,
			"jobID":          tasks[0].Def.JobID,
		}).Infof("Assigned %d tasks", len(assignments))
	return assignments
}

type nodeStatesByNodeID map[cluster.NodeId]*nodeState

// Helper fn, appends to 'assignments' and updates nodeGroups.
// Should successfully assign all given tasks if caller invokes this with self-consistent params.
// Note: there may be a race condition between recognizing idle nodes as available for assignment and
// nodes becoming offlined or suspended. The code does the best it can, but it may assign a task to
// a node that clusterState considers offlined/suspended before or as the task is actually being started
func (s *statefulScheduler) assign(tasks []*taskState) (assignments []taskAssignment) {
	idleNodesByGroupIDs := map[string]nodeStatesByNodeID{}

	// make a local copy of (non-suspended/offlined) idle nodeIDs by groupID, these will be the nodes assigned to the tasks
	for groupID, group := range s.clusterState.nodeGroups {
		if len(group.idle) > 0 {
			idleNodesByGroupIDs[groupID] = nodeStatesByNodeID{}
			for _, ns := range group.idle {
				if !ns.suspended() && !s.clusterState.isOfflined(ns) {
					idleNodesByGroupIDs[groupID][ns.node.Id()] = ns
				}
			}
		}
	}

	for _, task := range tasks {
		var nodeSt *nodeState

		// is there a node group (with idle node) for this snapshot?
		if nodeGroup, ok := idleNodesByGroupIDs[task.Def.SnapshotID]; ok {
			nodeSt = s.findIdleNodeInGroup(nodeGroup)
		}
		if nodeSt == nil {
			// could not find any free nodes in node group for the task's snapshot id.  Look for a free node in the other
			// node groups, starting with the "" node group
			if nodeGroup, ok := idleNodesByGroupIDs[""]; ok {
				nodeSt = s.findIdleNodeInGroup(nodeGroup)
			}
			if nodeSt == nil {
				for groupID, nodeGroup := range idleNodesByGroupIDs {
					if groupID == "" {
						continue
					}
					nodeSt = s.findIdleNodeInGroup(nodeGroup)
					if nodeSt != nil {
						break
					}
				}
			}
		}

		// Could not find any more free nodes
		if nodeSt == nil {
			log.WithFields(
				log.Fields{
					"jobID":  task.JobId,
					"taskID": task.TaskId,
					"tag":    task.Def.Tag,
				}).Warn("Unable to assign, no free node for task")
			continue
		}
		assignments = append(assignments, taskAssignment{nodeSt: nodeSt, task: task})

		// Mark Task as Started in the cluster
		s.clusterState.taskScheduled(nodeSt.node.Id(), task.JobId, task.Def.TaskID, task.Def.SnapshotID)

		log.WithFields(
			log.Fields{
				"jobID":          task.JobId,
				"taskID":         task.TaskId,
				"node":           nodeSt.node,
				"numAssignments": len(assignments),
				"tag":            task.Def.Tag,
			}).Info("Scheduling task")
		s.stat.Counter(stats.SchedScheduledTasksCounter).Inc(1)
	}
	return assignments
}

// findIdleNodeInGroup find a node in the group's idle nodes that is not suspended or offlined (this method will, pick
// up nodes that have been suspended/offlined while the processing was assigning other tasks to nodes).
// It also removes the node from the groups idle nodes list to prevent it from being assigned again.
func (s *statefulScheduler) findIdleNodeInGroup(nodeGroup nodeStatesByNodeID) *nodeState {
	for id, ns := range nodeGroup {
		if ns.suspended() || s.clusterState.isOfflined(ns) {
			continue
		}
		delete(nodeGroup, id)
		return ns
	}
	return nil
}

// Helpers.
func min(num int, nums ...int) int {
	m := num
	for _, n := range nums {
		if n < m {
			m = n
		}
	}
	return m
}
func max(num int, nums ...int) int {
	m := num
	for _, n := range nums {
		if n > m {
			m = n
		}
	}
	return m
}
func ceil(num float32) int {
	return int(math.Ceil(float64(num)))
}
