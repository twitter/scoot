package scheduler

import (
	"sort"

	log "github.com/Sirupsen/logrus"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/sched"
)

type taskAssignment struct {
	node    cluster.Node
	task    *taskState
	running *taskState
}

type KillableTasks []*taskState

func (k KillableTasks) Len() int           { return len(k) }
func (k KillableTasks) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k KillableTasks) Less(i, j int) bool { return k[i].TimeStarted.Before(k[j].TimeStarted) }

// Returns a list of taskAssigments of task to available node.
// Also returns a modified copy of clusterState.nodeGroups for the caller to apply (so this remains a pure fn).
// Note: pure fn because it's confusing to have getTaskAssignments() modify clusterState based on the proposed
//       scheduling and also require that the caller apply final modifications to clusterState as a second step)
//
// Does best effort scheduling which tries to assign tasks to nodes already primed for similar tasks.
// Not all tasks are guaranteed to be scheduled.
func getTaskAssignments(cs *clusterState, jobs []*jobState, requestors map[string][]*jobState) (
	[]taskAssignment, map[string]*nodeGroup,
) {
	// Exit if there are no unscheduled tasks.
	totalUnschedTasks := 0
	for _, j := range jobs {
		totalUnschedTasks += (len(j.Tasks) - j.TasksCompleted - j.TasksRunning)
	}
	if totalUnschedTasks == 0 {
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

	// Sort jobs by priority and count running tasks.
	// TOOD(jschiller): don't kill tasks that will result in more than some threshold of work being discarded.
	//
	// An array indexed by priority. The value is the total number of running tasks for jobs of the given priority.
	numKillableTasks := []int{0, 0, 0, 0}
	// An array indexed by priority. The value is the subset of jobs in fifo order for the given priority.
	priorityJobs := [][]*jobState{[]*jobState{}, []*jobState{}, []*jobState{}, []*jobState{}}
	for _, job := range jobs {
		p := int(job.Job.Def.Priority)
		numKillableTasks[p] += job.TasksRunning
		priorityJobs[p] = append(priorityJobs[p], job)
	}

	// List killable tasks first by ascending priority and within that, by ascending execution duration.
	//
	// An array of *tasksState ordered by kill preference. Omits priority=3 since nothing should kill those tasks.
	killableTasks := KillableTasks{}
	for p := range []sched.Priority{sched.P0, sched.P1, sched.P2} {
		ts := KillableTasks{}
		for _, j := range priorityJobs[p] {
			copy(ts, j.Tasks)
		}
		sort.Sort(ts)
		killableTasks = append(killableTasks, ts...)
	}

	// Assign each job the minimum number of nodes until idle nodes, and killable nodes if allowed, are exhausted.
	// Priority3 jobs consume all available idle+killable nodes and starve jobs of a lower priority
	// Priority2 jobs consume all remaining idle+killable nodes up to a limit, then give lower priority jobs a chance.
	// Priority1 jobs consume all remaining idle nodes up to a limit, and are preferred of Priority0 jobs.
	// Priority0 jobs consume all remaining idle nodes up to a limit.
	//
	var tasks []*taskState
	nk := numKillableTasks
	// The number of healthy nodes we can assign before killing tasks on other nodes.
	numIdle := cs.numIdle
	// A map[requestor]map[tag]bool{} that makes sure we process all tags for a given requestor once as a batch.
	requestorTagsSeen := map[string]map[string]bool{}
	// An array indexed by priority. The value is the number of tasks that a job of the given priority can kill.
	// Only priority=3 and priority=2 jobs can kill other tasks (note, killable tasks are double counted here).
	numKillableCounter := []int{0, 0, (nk[0] + nk[1]), (nk[0] + nk[1] + nk[2])}
	// An array indexed by priority. The value is a list of jobs, each with a list of tasks yet to be scheduled.
	remaining := [][][]*taskState{[][]*taskState{}, [][]*taskState{}, [][]*taskState{}, [][]*taskState{}}
Loop:
	for _, p := range []sched.Priority{sched.P3, sched.P2, sched.P1, sched.P0} {
		for _, job := range priorityJobs[p] {
			// The number of available nodes for this priority is the remaining idle nodes plus allowed killable nodes.
			numAvailNodes := numIdle + numKillableCounter[p]
			if numAvailNodes == 0 {
				break Loop
			}

			// If we've seen this tag for this requestor before then it's already been handled, so skip this job.
			def := &job.Job.Def
			if tags, ok := requestorTagsSeen[def.Requestor]; ok {
				if _, ok := tags[def.Tag]; ok {
					continue
				}
			} else {
				requestorTagsSeen[def.Requestor] = map[string]bool{}
			}
			requestorTagsSeen[def.Requestor][def.Tag] = true

			// Find all jobs with the same requestor/tag combination and add their unscheduled tasks to 'unsched'.
			// Also keep track of how many total tasks were requested for these jobs and how many are currently running.
			numTasks := 0
			numRunning := 0
			unsched := []*taskState{}
			for _, j := range requestors[def.Requestor] {
				if j.Job.Def.Tag == def.Tag {
					numTasks += len(j.Tasks)
					numRunning += j.TasksRunning
					unsched = append(j.getUnScheduledTasks(), unsched...)
					// Stop checking for unscheduled tasks if they exceed available nodes (we'll cap it below).
					if len(unsched) > numAvailNodes {
						break
					}
				}
			}
			// No unscheduled tasks, continue onto the next job.
			if len(unsched) == 0 {
				continue
			}

			// How many of the requested tasks can we assign based on the max healthy task load for our cluster.
			numScaledTasks := ceil(float32(numTasks) * NodeScaleFactor)
			numSchedulable := 0
			if p == sched.P3 {
				// Priority=3 jobs always get the maximum number of available nodes, as needed, and in fifo order.
				numSchedulable = min(len(unsched), numAvailNodes)
			} else {
				// Get the lesser of the number of unscheduled tasks and number of available nodes.
				// Further, get the lesser of that, the healthy task load, and default number of nodes to run a large job.
				numSchedulable = min(len(unsched), numAvailNodes, numScaledTasks, LargeJobMaxNodes)
				// The number of tasks we can schedule is reduced by the number of tasks we're already running.
				numSchedulable = max(0, numSchedulable-numRunning)
			}
			if numSchedulable > 0 {
				log.Infof("Job:%s, priority:%d, numSchedulable:%d, numRunning:%d", job.Job.Id, p, numSchedulable, numRunning)
				log.Debugf("Job:%s, min(unsched:%d, numAvailNodes:%d, numScaledTasks:%d, largeJobMaxNodes:%d) - numRunning:%d",
					job.Job.Id, len(unsched), numAvailNodes, numScaledTasks, LargeJobMaxNodes, numRunning)
				tasks = append(tasks, unsched[0:numSchedulable]...)
				// Get the number of nodes we can take from the idle pool, and the number we must take from killable nodes.
				numFromIdle := min(numIdle, numSchedulable)
				numFromKill := max(0, numSchedulable-numFromIdle)
				// Deduct from the number of idle nodes - this value gets used after we exit this loop.
				numIdle -= numFromIdle
				// If there weren't enough idle nodes, grab more from the appropriate pool of killable nodes.
				// Note that numSchedulable should not exceed numAvailNodes so we don't do any checking for that.
				if numFromKill > 0 && p == sched.P3 {
					// For priority=3, deduct from the p3 counter and update the p2 counter to account for it.
					numFromP2 := numFromKill - numKillableCounter[sched.P3]
					numKillableCounter[sched.P3] -= min(numKillableCounter[sched.P3], numFromKill)
					numKillableCounter[sched.P2] -= max(0, numFromP2)
				} else if numFromKill > 0 && p == sched.P2 {
					// For priority=2, deduct from the p2 counter (and the p1 and p0 counters aren't used).
					numKillableCounter[sched.P2] -= numFromKill
				}
			}

			// We are unable to assign more nodes at this priority.
			// Update remaining - append an array containing all unscheduled tasks for this requestor/tag combination.
			remaining[p] = append(remaining[p], unsched[numSchedulable:])
		}
	}

	// If there are spare idle nodes, priority=3 jobs have been satisfied already.
	// Distribute a minimum of 50% idle to priority=2, and 25% each to priority=1 and priority=0
	// TODO(jschiller) percentages should be configurable. Not super important as this will be infrequently exercised.
LoopRemaining:
	// First loop using the above percentages, and next, distribute remaining idle nodes to the highest priority tasks.
	for _, pq := range [][]float32{[]float32{.5, .25, .25}, []float32{1, 1, 1}} {
		for _, p := range []sched.Priority{sched.P2, sched.P1, sched.P0} {
			if numIdle == 0 {
				break LoopRemaining
			}
			// The remaining tasks, bucketed by job, for a given priority.
			taskLists := remaining[p]
			// Distribute the allowed number of idle nodes evenly across the bucketed jobs for a given priority.
			nodeQuota := ceil((float32(numIdle) * pq[p]) / float32(len(taskLists)))
			for i, taskList := range taskLists {
				// Noting that we use ceil() above, we may use less quota than assigned if it's unavailable or unneeded.
				nTasks := min(numIdle, nodeQuota, len(taskList))
				if nTasks > 0 {
					// Move the given number of tasks from remaining to the list of tasks that will be assigned nodes.
					log.Infof("Assigning %d additional idle nodes to tasks with priority=%d", nTasks, p)
					numIdle -= nTasks
					tasks = append(tasks, taskList[:nTasks]...)
					taskLists[i] = taskList[nTasks:]
					// Remove jobs that have run out of runnable tasks.
					if len(taskLists[i]) == 0 {
						remaining[p] = append(taskLists[:i], taskLists[i+1:]...)
					}
				}
			}
		}
	}
	// Exit if no tasks qualify to be scheduled.
	if len(tasks) == 0 {
		return nil, nil
	}

	// Loop over all cluster snapshotIds looking for an idle node. Prefer, in order:
	// - Hot node for the given snapshotId (one whose last task shared the same snapshotId).
	// - New untouched node (or node whose last task used an empty snapshotId)
	// - A random node from the idle pools of nodes associated with other snapshotIds.
	assignments := assign(cs, tasks, killableTasks, nodeGroups, append([]string{""}, clusterSnapshotIds...))
	if len(assignments) == len(tasks) {
		log.Infof("Scheduled all tasks (%d)", len(tasks))
	} else {
		log.Infof("Unable to schedule all tasks, scheduled=%d/%d", len(assignments), len(tasks))
	}
	return assignments, nodeGroups
}

// Helper fn, appends to 'assignments' and updates nodeGroups.
// Should successfully assign all given tasks if caller invokes this with self-consistent params.
func assign(
	cs *clusterState,
	tasks []*taskState,
	killableTasks KillableTasks,
	nodeGroups map[string]*nodeGroup,
	snapIds []string,
) (assignments []taskAssignment) {
	for _, task := range tasks {
		var snapshotId string
		var nodeSt *nodeState
		var wasRunning *taskState
	SnapshotsLoop:
		for _, snapId := range append([]string{task.Def.SnapshotID}, snapIds...) {
			if groups, ok := nodeGroups[snapId]; ok {
				for _, ns := range groups.idle {
					if ns.suspended() {
						continue
					}
					snapshotId = snapId
					nodeSt = ns
					break SnapshotsLoop
				}
			}
		}
		// Could not find any more idle nodes, take one from killable nodes.
		if nodeSt == nil {
			wasRunning = killableTasks[0]
			snapshotId = wasRunning.Def.SnapshotID
			nodeSt = cs.nodes[wasRunning.TaskRunner.nodeId]
			killableTasks = killableTasks[1:]

		}
		assignments = append(assignments, taskAssignment{node: nodeSt.node, task: task, running: wasRunning})
		if _, ok := nodeGroups[snapshotId]; !ok {
			nodeGroups[snapshotId] = newNodeGroup()
		}
		nodeId := nodeSt.node.Id()
		nodeGroups[snapshotId].busy[nodeId] = nodeSt
		delete(nodeGroups[snapshotId].idle, nodeId)
		log.Infof("Scheduled jobId=%s, taskId=%s, node=%s, progress=%d/%d",
			task.JobId, task.TaskId, nodeId, len(assignments), len(assignments)+len(tasks))
	}
	return assignments
}
