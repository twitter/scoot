package scheduler

import (
	log "github.com/Sirupsen/logrus"

	"github.com/scootdev/scoot/cloud/cluster"
)

type taskAssignment struct {
	node        cluster.Node
	task        *taskState
	runningTask *taskState
}

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

	// Sort jobs by priority.
	var tasks []*taskState
	numIdle := cs.numIdle
	requestorTagsSeen := map[string]map[string]bool{}
	numRunningTasks := map[int]int{0: 0, 1: 0, 2: 0, 3: 0}
	priorityJobs := map[int][]*jobState{0: []*jobState{}, 1: []*jobState{}, 2: []*jobState{}, 3: []*jobState{}}
	for _, job := range jobs {
		p := min(1, int(job.Job.Def.Priority)) //TODO(jschiller): delete this. Add priority=2 and priority=3 task-killing
		numRunningTasks[p] += job.TasksRunning
		priorityJobs[p] = append(priorityJobs[p], job)
	}

	// Assign each job the minimum number of nodes until idle nodes are exhausted.
	remaining := map[int][][]*taskState{0: [][]*taskState{}, 1: [][]*taskState{}, 2: [][]*taskState{}, 3: [][]*taskState{}}
Loop:
	for p := range []int{1, 0} {
		for _, job := range priorityJobs[p] {
			def := &job.Job.Def
			if numIdle == 0 {
				break Loop
			}
			if tags, ok := requestorTagsSeen[def.Requestor]; ok {
				if _, ok := tags[def.Tag]; ok {
					continue
				}
			} else {
				requestorTagsSeen[def.Requestor] = map[string]bool{}
			}
			requestorTagsSeen[def.Requestor][def.Tag] = true

			numTasks := 0
			numRunning := 0
			unsched := []*taskState{}
			for _, j := range requestors[def.Requestor] {
				if j.Job.Def.Tag == def.Tag {
					numTasks += len(j.Tasks)
					numRunning += j.TasksRunning
					unsched = append(j.getUnScheduledTasks(), unsched...)
				}
			}

			numSchedulable := min(ceil(float32(numTasks)*NodeScaleFactor), len(unsched), numIdle, DefaultMinNodes)
			numSchedulable -= numRunning
			if numSchedulable > 0 {
				tasks = append(tasks, unsched[0:numSchedulable]...)
				numIdle -= numSchedulable
			}

			remaining[p] = append(remaining[p], unsched[numSchedulable:])
		}
	}

	// If there are spare idle nodes, priority=3 jobs have been satisfied already.
	// Assign a minimum of 50% to priority=2, and 25% each to priority=1 and priority=0
LoopRemaining:
	for _, pq := range [][]float32{[]float32{.5, .25, .25}, []float32{1, 1, 1}} {
		for _, p := range []int{2, 1, 0} {
			if numIdle == 0 {
				break LoopRemaining
			}
			taskLists := remaining[p]
			nodeQuota := ceil((float32(numIdle) * pq[p]) / float32(len(taskLists)))
			for _, taskList := range taskLists {
				nq := min(numIdle, nodeQuota, len(taskList))
				if nq > 0 {
					tasks = append(tasks, taskList[:nq]...)
					numIdle -= nq
				}
			}
		}
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
// Returns tasks that couldn't be scheduled using the task's snapshotId or any of those in snapIds.
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
					if ns.suspended() {
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
