package server

import (
	"math"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/stats"
)

type taskAssignment struct {
	nodeSt *nodeState
	task   *taskState
}

// Returns a list of taskAssigments of task to free node.
// Also returns a modified copy of clusterState.nodeGroups for the caller to apply (so this remains a pure fn).
// Note: pure fn because it's confusing to have getTaskAssignments() modify clusterState based on the proposed
//       scheduling and also require that the caller apply final modifications to clusterState as a second step)
//
// Does best effort scheduling which tries to assign tasks to nodes already primed for similar tasks.
// Not all tasks are guaranteed to be scheduled.
func (s *statefulScheduler) getTaskAssignments() []taskAssignment {
	cs := s.clusterState
	jobs := s.inProgressJobs
	requestors := s.requestorMap
	stat := s.stat

	if stat == nil {
		stat = stats.NilStatsReceiver()
	}
	defer s.stat.Latency(stats.SchedTaskAssignmentsLatency_ms).Time().Stop()

	// Exit if there are no unscheduled tasks.
	unscheduledTasks := false
	for _, j := range jobs {
		if len(j.NotStarted) > 0 {
			unscheduledTasks = true
			break
		}
	}
	if !unscheduledTasks {
		return nil
	}

	tasks, stopTasks := s.config.SchedAlg.GetTasksToBeAssigned(jobs, stat, cs, requestors)
	// Exit if no tasks qualify to be scheduled.
	if len(tasks) == 0 {
		return nil
	}

	// stop the tasks in stopTasks (we are rebalancing the workers)
	log.WithFields(log.Fields{"numTasks": len(stopTasks)}).Info("stopping tasks")
	for _, task := range stopTasks {
		jobState := s.getJob(task.JobId)
		logFields := log.Fields{
			"jobID":     task.JobId,
			"requestor": jobState.Job.Def.Requestor,
			"jobType":   jobState.Job.Def.JobType,
			"tag":       jobState.Job.Def.Tag,
		}
		msgs := s.abortTask(jobState, task, logFields)
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
	assignments := assign(cs, tasks, stat)
	log.WithFields(
		log.Fields{
			"numAssignments": len(assignments),
			"numTasks":       len(tasks),
			"tag":            tasks[0].Def.Tag,
			"jobID":          tasks[0].Def.JobID,
		}).Infof("Scheduled %d tasks", len(assignments))
	return assignments
}

// Helper fn, appends to 'assignments' and updates nodeGroups.
// Should successfully assign all given tasks if caller invokes this with self-consistent params.
func assign(
	cs *clusterState,
	tasks []*taskState,
	stat stats.StatsReceiver,
) (assignments []taskAssignment) {
	idleNodeGroupIDs := map[string]string{}
	for groupID, group := range cs.nodeGroups {
		if len(group.idle) > 0 {
			idleNodeGroupIDs[groupID] = groupID
		}
	}
	for _, task := range tasks {
		var idleNodeGroupID string
		var nodeSt *nodeState

		// is there a node group (with idle node) for this snapshot?
		if _, ok := cs.nodeGroups[task.Def.SnapshotID]; ok {
			nodeSt = findIdleNodeInGroup(cs, task.Def.SnapshotID)
		}
		if nodeSt != nil {
			idleNodeGroupID = task.Def.SnapshotID
		} else {
			// could not find any free nodes in node group for the task's snapshot id.  Look for a free node in the other
			// node groups, starting with the "" node group
			if _, ok := idleNodeGroupIDs[""]; ok {
				nodeSt = findIdleNodeInGroup(cs, "")
			}
			if nodeSt == nil {
				for groupID := range idleNodeGroupIDs {
					if groupID == "" {
						continue
					}
					nodeSt = findIdleNodeInGroup(cs, groupID)
					if nodeSt != nil {
						idleNodeGroupID = groupID
						break
					}
				}
			}
		}
		// Could not find any free nodes
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

		if _, ok := cs.nodeGroups[task.Def.SnapshotID]; !ok {
			cs.nodeGroups[task.Def.SnapshotID] = newNodeGroup()
		}
		nodeID := nodeSt.node.Id()
		if idleNodeGroupID != task.Def.SnapshotID {
			// move the idle node to the task's node group, taskScheduled (called below) moves the nodesSt
			// from the task group's idle node list to busy
			delete(cs.nodeGroups[idleNodeGroupID].idle, nodeID)
			cs.nodeGroups[task.Def.SnapshotID].idle[nodeID] = nodeSt
			nodeSt.snapshotId = task.Def.SnapshotID
			// if all nodes have been moved from this node group, delete the nodeGroup from the clusterState nodeGroups map
			if len(cs.nodeGroups[idleNodeGroupID].idle) == 0 && len(cs.nodeGroups[idleNodeGroupID].busy) == 0 {
				delete(cs.nodeGroups, idleNodeGroupID)
				if _, ok := idleNodeGroupIDs[idleNodeGroupID]; ok {
					delete(idleNodeGroupIDs, idleNodeGroupID)
				}
			}
		}
		// Mark Task as Started in the cluster
		cs.taskScheduled(nodeSt.node.Id(), task.JobId, task.Def.TaskID, task.Def.SnapshotID)

		log.WithFields(
			log.Fields{
				"jobID":          task.JobId,
				"taskID":         task.TaskId,
				"node":           nodeSt.node,
				"numAssignments": len(assignments),
				"tag":            task.Def.Tag,
			}).Info("Scheduling task")
		stat.Counter(stats.SchedScheduledTasksCounter).Inc(1)
	}
	return assignments
}

func findIdleNodeInGroup(cs *clusterState, groupID string) *nodeState {
	for _, ns := range cs.nodeGroups[groupID].idle {
		if ns.suspended() || cs.isOfflined(ns) {
			continue
		}
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
