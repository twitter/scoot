package scheduler

import (
	"sort"

	log "github.com/Sirupsen/logrus"

	"github.com/scootdev/scoot/cloud/cluster"
)

type taskAssignment struct {
	node        cluster.Node
	task        *taskState
	runningTask *taskState
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

	// Sort jobs by priority.
	var tasks []*taskState
	numIdle := cs.numIdle - len(cs.suspendedNodes)
	requestorTagsSeen := map[string]map[string]bool{}
	numKillableTasks := []int{0, 0, 0, 0}
	priorityJobs := [][]*jobState{[]*jobState{}, []*jobState{}, []*jobState{}, []*jobState{}}
	for _, job := range jobs {
		p := int(job.Job.Def.Priority)
		numKillableTasks[p] += job.TasksRunning
		priorityJobs[p] = append(priorityJobs[p], job)
	}

	// List killable tasks first by ascending priority and next by ascending execution duration.
	killableTasks := KillableTasks{}
	for p := range []int{0, 1, 2} {
		ts := KillableTasks{}
		for _, j := range priorityJobs[p] {
			copy(ts, j.Tasks)
		}
		sort.Sort(ts)
		killableTasks = append(killableTasks, ts...)
	}

	// Assign each job the minimum number of nodes until idle nodes are exhausted.
	// Remaining: indexed by priority and contains a list of jobs, each with a list of tasks.
	nk := numKillableTasks
	numKillableCounter := []int{0, 0, (nk[0] + nk[1]), (nk[0] + nk[1] + nk[2])}
	remaining := [][][]*taskState{[][]*taskState{}, [][]*taskState{}, [][]*taskState{}, [][]*taskState{}}
Loop:
	for p := range []int{3, 2, 1, 0} {
		for _, job := range priorityJobs[p] {
			numAvail := numIdle + numKillableCounter[p]
			def := &job.Job.Def
			if numAvail == 0 {
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

			numSchedulable := min(ceil(float32(numTasks)*NodeScaleFactor), len(unsched), numAvail, DefaultMinNodes)
			numSchedulable -= numRunning
			if numSchedulable > 0 {
				tasks = append(tasks, unsched[0:numSchedulable]...)
				numFromIdle := min(numIdle, numSchedulable)
				numFromKill := numSchedulable - numFromIdle
				numIdle -= numFromIdle
				if p == 3 {
					numFromP2 := max(0, numFromKill-numKillableCounter[3])
					numKillableCounter[3] -= min(numKillableCounter[p], numFromKill)
					numKillableCounter[2] -= numFromP2
				} else if p == 2 {
					numKillableCounter[2] -= numFromKill
				}
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
// Should successfully assign all given
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
		if task.OwnerJob.Job.Def.Priority >= 2 {
			snapshotId = killableTasks[0].Def.SnapshotID
			nodeSt = cs.nodes[killableTasks[0].Runner.nodeId]
		}
		assignments = append(assignments, taskAssignment{node: nodeSt.node, task: task})
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
