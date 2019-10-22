package scheduler

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
func getTaskAssignments(cs *clusterState, jobs []*jobState,
	requestors map[string][]*jobState, config *SchedulerConfig, stat stats.StatsReceiver,
	schedAlg SchedulingAlgorithm) (
	[]taskAssignment, map[string]*nodeGroup,
) {
	if stat == nil {
		stat = stats.NilStatsReceiver()
	}
	defer stat.Latency(stats.SchedTaskAssignmentsLatency_ms).Time().Stop()

	// Exit if there are no unscheduled tasks.
	totalOutstandingTasks := 0
	totalUnschedTasks := 0
	for _, j := range jobs {
		totalOutstandingTasks += (len(j.Tasks) - j.TasksCompleted)
		totalUnschedTasks += (len(j.Tasks) - j.TasksCompleted - j.TasksRunning)
	}
	if totalUnschedTasks == 0 {
		return nil, nil
	}

	// Udate SoftMaxSchedulableTasks based on number of healthy nodes and the total number of tasks.
	// Setting the max to num healthy nodes means that each job can be fully scheduled.
	// (This gets used in config.GetNodeScaleFactor())
	cfg := SchedulerConfig{SoftMaxSchedulableTasks: max(totalOutstandingTasks, len(cs.nodes))}
	if config != nil {
		cfg = *config
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

	tasks := schedAlg.GetTasksToBeAssigned(jobs, stat, cs, requestors, cfg)
	// Exit if no tasks qualify to be scheduled.
	if len(tasks) == 0 {
		return nil, nil
	}

	// Loop over all cluster snapshotIds looking for a usable node. Prefer, in order:
	// - Hot node for the given snapshotId (one whose last task shared the same snapshotId).
	// - New untouched node (or node whose last task used an empty snapshotId)
	// - A random free node from the idle pools of nodes associated with other snapshotIds.
	assignments := assign(cs, tasks, nodeGroups, append([]string{""}, clusterSnapshotIds...), stat)
	if len(assignments) == totalUnschedTasks {
		log.WithFields(
			log.Fields{
				"numTasks": len(tasks),
				"tag":      tasks[0].Def.Tag,
				"jobID":    tasks[0].Def.JobID,
			}).Info("Scheduled all tasks")
	} else {
		log.WithFields(
			log.Fields{
				"numAssignments": len(assignments),
				"numTasks":       totalUnschedTasks,
				"tag":            tasks[0].Def.Tag,
				"jobID":          tasks[0].Def.JobID,
			}).Info("Unable to schedule all tasks")
	}
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
			}).Info("Scheduled job")
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
