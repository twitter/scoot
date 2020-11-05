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
func (s *statefulScheduler) getTaskAssignments() (
	[]taskAssignment, map[string]*nodeGroup,
) {
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
		return nil, nil
	}

	// Create a copy of cs.nodeGroups to modify based on new scheduling.
	clusterSnapshotIds := []string{}
	nodeGroups := map[string]*nodeGroup{}
	for snapId, groups := range cs.nodeGroups {
		nodeGroups[snapId] = newNodeGroup()
		for nodeId, node := range groups.idle {
			nodeGroups[snapId].idle[nodeId] = node
		}
		for nodeId, node := range groups.busy {
			nodeGroups[snapId].busy[nodeId] = node
		}
		clusterSnapshotIds = append(clusterSnapshotIds, snapId)
	}

	tasks, stopTasks := s.config.SchedAlg.GetTasksToBeAssigned(jobs, stat, cs, requestors)
	// Exit if no tasks qualify to be scheduled.
	if len(tasks) == 0 {
		return nil, nil
	}

	// stop the tasks in stopTasks (we are rebalancing the workers)
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
	assignments := assign(cs, tasks, nodeGroups, append([]string{""}, clusterSnapshotIds...), stat)
	log.WithFields(
		log.Fields{
			"numAssignments": len(assignments),
			"numTasks":       len(tasks),
			"tag":            tasks[0].Def.Tag,
			"jobID":          tasks[0].Def.JobID,
		}).Infof("Scheduled %d tasks", len(assignments))
	return assignments, nodeGroups
}

// Helper fn, appends to 'assignments' and updates nodeGroups.
// Should successfully assign all given tasks if caller invokes this with self-consistent params.
func assign(
	cs *clusterState,
	tasks []*taskState,
	nodeGroups map[string]*nodeGroup,
	snapIds []string,
	stat stats.StatsReceiver,
) (assignments []taskAssignment) {
	for _, task := range tasks {
		var snapshotId string
		var nodeSt *nodeState
	SnapshotsLoop:
		for _, snapId := range append([]string{task.Def.SnapshotID}, snapIds...) {
			if groups, ok := nodeGroups[snapId]; ok {
				for _, ns := range groups.idle {
					if ns.suspended() || cs.isOfflined(ns) {
						continue
					}
					snapshotId = snapId
					nodeSt = ns
					break SnapshotsLoop
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
		if _, ok := nodeGroups[snapshotId]; !ok {
			nodeGroups[snapshotId] = newNodeGroup()
		}
		nodeId := nodeSt.node.Id()
		nodeGroups[snapshotId].busy[nodeId] = nodeSt
		delete(nodeGroups[snapshotId].idle, nodeId)
		log.WithFields(
			log.Fields{
				"jobID":          task.JobId,
				"taskID":         task.TaskId,
				"node":           nodeSt.node,
				"numAssignments": len(assignments),
				"numTasks":       len(tasks),
				"tag":            task.Def.Tag,
			}).Info("Scheduling task")
		stat.Counter(stats.SchedScheduledTasksCounter).Inc(1)
	}
	return assignments
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
