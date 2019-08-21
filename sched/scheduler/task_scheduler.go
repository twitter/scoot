package scheduler

import (
	"math"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/sched"
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
	requestors map[string][]*jobState, config *SchedulerConfig, stat stats.StatsReceiver) (
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

	// Sort jobs by priority and count running tasks.
	// An array indexed by priority. The value is the subset of jobs in fifo order for the given priority.
	priorityJobs := [][]*jobState{[]*jobState{}, []*jobState{}, []*jobState{}}
	for _, job := range jobs {
		p := int(job.Job.Def.Priority)
		priorityJobs[p] = append(priorityJobs[p], job)
	}
	stat.Gauge(stats.SchedPriority0JobsGauge).Update(int64(len(priorityJobs[sched.P0])))
	stat.Gauge(stats.SchedPriority1JobsGauge).Update(int64(len(priorityJobs[sched.P1])))
	stat.Gauge(stats.SchedPriority2JobsGauge).Update(int64(len(priorityJobs[sched.P2])))

	// Assign each job the minimum number of nodes until free nodes are exhausted.
	// Priority2 jobs consume all remaining idle nodes up to a limit, and are preferred over Priority1 jobs.
	// Priority1 jobs consume all remaining idle nodes up to a limit, and are preferred over Priority0 jobs.
	// Priority0 jobs consume all remaining idle nodes up to a limit.
	//
	var tasks []*taskState
	// The number of healthy nodes we can assign
	numFree := cs.numFree()
	// A map[requestor]map[tag]bool{} that makes sure we process all tags for a given requestor once as a batch.
	tagsSeen := map[string]map[string]bool{}
	// An array indexed by priority. The value is a list of jobs, each with a list of tasks yet to be scheduled.
	// 'Optional' means tasks are associated with jobs that are already running with some minimum node quota.
	// 'Required' means tasks are associated with jobs that haven't yet reach a minimum node quota.
	// This distinction makes sure we don't starve lower priority jobs in the second-pass 'remaining' loop.
	remainingOptional := [][][]*taskState{[][]*taskState{}, [][]*taskState{}, [][]*taskState{}}
	remainingRequired := [][][]*taskState{[][]*taskState{}, [][]*taskState{}, [][]*taskState{}}
Loop:
	for _, p := range []sched.Priority{sched.P2, sched.P1, sched.P0} {
		for _, job := range priorityJobs[p] {
			// The number of available nodes for this priority is the remaining free nodes
			numAvailNodes := numFree
			if numAvailNodes == 0 {
				break Loop
			}

			// If we've seen this tag for this requestor before then it's already been handled, so skip this job.
			def := &job.Job.Def
			if tags, ok := tagsSeen[def.Requestor]; ok {
				if _, ok := tags[def.Tag]; ok {
					continue
				}
			} else {
				tagsSeen[def.Requestor] = map[string]bool{}
			}
			tagsSeen[def.Requestor][def.Tag] = true

			// Find all jobs with the same requestor/tag combination and add their unscheduled tasks to 'unsched'.
			// Also keep track of how many total tasks were requested for these jobs and how many are currently running.
			// If later jobs in this group have a higher priority, we handle it by scheduling those first within the group.
			numTasks := 0
			numRunning := 0
			numCompleted := 0
			unsched := []*taskState{}
			for _, j := range requestors[def.Requestor] {
				if j.Job.Def.Tag == def.Tag {
					numTasks += len(j.Tasks)
					numCompleted += j.TasksCompleted
					numRunning += j.TasksRunning
					// Prepend tasks to handle the likely desire for immediate retries for failed tasks in a previous job.
					// Hardcoded for now as this is the way scheduler is currently invoked by customers.
					unsched = append(j.getUnScheduledTasks(), unsched...)
					// Stop checking for unscheduled tasks if they exceed available nodes (we'll cap it below).
					if len(unsched) >= numAvailNodes {
						break
					}
				}
			}
			// No unscheduled tasks, continue onto the next job.
			if len(unsched) == 0 {
				continue
			}

			// How many of the requested tasks can we assign based on the max healthy task load for our cluster.
			// (the schedulable count is the minimum number of nodes appropriate for the current set of tasks).
			numScaledTasks := ceil(float32(numTasks) * cfg.GetNodeScaleFactor(len(cs.nodes), p))
			numSchedulable := 0
			numDesiredUnmet := 0

			// For this group of tasks we want the lesser of: the number remaining, or a number based on load.
			numDesired := min(len(unsched), numScaledTasks)
			// The number of tasks we can schedule is reduced by the number of tasks we're already running.
			// Further the number we'd like and the number we'll actually schedule are restricted by numAvailNodes.
			// If numDesiredUnmet==0 then any remaining outstanding tasks are low priority and appended to remainingOptional
			numDesiredUnmet = max(0, numDesired-numRunning-numAvailNodes)
			numSchedulable = min(numAvailNodes, max(0, numDesired-numRunning))

			if numSchedulable > 0 {
				log.WithFields(
					log.Fields{
						"jobID":          job.Job.Id,
						"priority":       p,
						"numTasks":       numTasks,
						"numSchedulable": numSchedulable,
						"numRunning":     numRunning,
						"numCompleted":   numCompleted,
						"tag":            job.Job.Def.Tag,
					}).Info("Schedulable tasks")
				log.WithFields(
					log.Fields{
						"jobID":           job.Job.Id,
						"unsched":         len(unsched),
						"numAvailNodes":   numAvailNodes,
						"numScaledTasks":  numScaledTasks,
						"numDesiredUnmet": numDesiredUnmet,
						"numRunning":      numRunning,
						"tag":             job.Job.Def.Tag,
					}).Debug("Schedulable tasks dbg")
				tasks = append(tasks, unsched[0:numSchedulable]...)
				// Get the number of nodes we can take from the free node pool
				numFromFree := min(numFree, numSchedulable)
				// Deduct from the number of free nodes - this value gets used after we exit this loop.
				numFree -= numFromFree
			}

			// To both required and optional, append an array containing unscheduled tasks for this requestor/tag combination.
			if numDesiredUnmet > 0 {
				remainingRequired[p] = append(remainingRequired[p], unsched[numSchedulable:numSchedulable+numDesiredUnmet])
			}
			if numSchedulable+numDesiredUnmet < len(unsched) {
				remainingOptional[p] = append(remainingOptional[p], unsched[numSchedulable+numDesiredUnmet:])
			}
		}
	}

	// Distribute a minimum of 75% free to priority=2, 20% to priority=1 and 5% to priority=0
	numFreeBasis := numFree
LoopRemaining:
	// First, loop using the above percentages, and next, distribute remaining free nodes to the highest priority tasks.
	// Do the above loops twice, once to exhaust 'required' tasks and again to exhaust 'optional' tasks.
	for j, quota := range [][]float32{NodeScaleAdjustment, []float32{1, 1, 1}, NodeScaleAdjustment, []float32{1, 1, 1}} {
		remaining := &remainingRequired
		if j > 1 {
			remaining = &remainingOptional
		}
		for _, p := range []sched.Priority{sched.P2, sched.P1, sched.P0} {
			if numFree == 0 {
				break LoopRemaining
			}
			// The remaining tasks, bucketed by job, for a given priority.
			taskLists := &(*remaining)[p]
			// Distribute the allowed number of free nodes evenly across the bucketed jobs for a given priority.
			nodeQuota := ceil((float32(numFreeBasis) * quota[p]) / float32(len(*taskLists)))
			for i := 0; i < len(*taskLists); i++ {
				taskList := &(*taskLists)[i]
				// Noting that we use ceil() above, we may use less quota than assigned if it's unavailable or unneeded.
				nTasks := min(numFree, nodeQuota, len(*taskList))
				if nTasks > 0 {
					// Move the given number of tasks from remaining to the list of tasks that will be assigned nodes.
					log.WithFields(
						log.Fields{
							"nTasks":   nTasks,
							"jobID":    (*taskList)[0].JobId,
							"priority": p,
							"numFree":  numFree,
							"tag":      (*taskList)[0].Def.Tag,
						}).Info("Assigning additional free nodes for each remaining task in job")
					numFree -= nTasks
					tasks = append(tasks, (*taskList)[:nTasks]...)
					// Remove jobs that have run out of runnable tasks.
					if len(*taskList)-nTasks > 0 {
						*taskList = (*taskList)[nTasks:]
					} else {
						*taskLists = append((*taskLists)[:i], (*taskLists)[i+1:]...)
						i--
					}
				}
			}
		}
	}
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
