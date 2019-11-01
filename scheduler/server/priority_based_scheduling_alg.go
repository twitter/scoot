package server

import (
	"fmt"
	"math"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/stats"
)

type PriorityBasedAlg struct {
	PriorityRatios           []int // array of priority ratios where array index == priority
	taskModCounter           int
	jobsByPriority           [][]*jobState // list of jobs at each priority
	roundRobinJobIndex       []int         // for each priority, track the index of the job whose task should be assigned next
	numBarredFromLowPriority int           // number of nodes reserved for non-lowest priority tasks
}

func MakePriorityBasedAlg(ratios []int) *PriorityBasedAlg {
	pbs := &PriorityBasedAlg{
		PriorityRatios: ratios,
		taskModCounter: 1,
	}

	return pbs
}

/*
This is the entry point to the scheduling algorithm
It returns the list of tasks that should be assigned to nodes as per the following description:

Use priority-based task run ratios, P0-Pn, where each task run ratio, Pi, has the semantic that we want to start a
task from a job with priority i, 1 out of every Pi tasks.
Eg: given task run ratios: [1, 10, 50]
 - jobs will have priorities 0, 1, 2 and
 - we will schedule
 - a task from a job with priority 2, 1 out of every 50 times a task is started,
 - a task from a job with priority 1, 1 out of every 10 times a task is started, otherwise
 - schedule a task from a job with priority 0 all other times a task is started
Note: the priority task run ratio array should be a list of ratios with increasing, non-repeating values - lower
priority job tasks run less frequently than higher priority job tasks.

The algorithm also uses a round robin approach to selecting tasks at a given priority level. For the jobs at a given
priority level, each time a task should be scheduled from that priority level, the algorithm picks one task from a job
at that priority level, then picks one task from all other jobs (at that priority level) before returning to select
another task from the original job.  This will prevent any one job from being stalled by other jobs.

When no tasks are available at the priority level whose turn it is, the algorithm will try to find a task at the next
higher priority level, repeatedly looking at the next highest priority level till it has reached priority 0.  If no
waiting task is still found, it will then look successively down the priority levels.
*/
func (pbs *PriorityBasedAlg) GetTasksToBeAssigned(jobs []*jobState, statNotUsed stats.StatsReceiver, cs *clusterState,
	jobsByRequestorNotUsed map[string][]*jobState, cfgNotUsed SchedulerConfig) []*taskState {
	idleNodeCnt := cs.numFree()
	pbs.numBarredFromLowPriority = 0 // TODO make this 10% of num nodes
	tasksToAssign := make([]*taskState, 0)
	selectedTasks := make(map[string]*taskState)

	// build the list of jobs at each priority
	for i := 0; i < len(pbs.PriorityRatios); i++ { // initialize priority to jobs array
		pbs.jobsByPriority = make([][]*jobState, len(pbs.PriorityRatios))
	}
	pbs.roundRobinJobIndex = make([]int, len(pbs.jobsByPriority))

	for _, job := range jobs {
		if job.TasksCompleted+job.TasksRunning < len(job.Tasks) {
			// the job still has tasks to assign
			priority := int(job.Job.Def.Priority)
			if pbs.jobsByPriority[priority] == nil {
				pbs.jobsByPriority[priority] = make([]*jobState, 0)
			}
			pbs.jobsByPriority[priority] = append(pbs.jobsByPriority[priority], job)
		}
	}

	// reset the priority based job round robin index
	for i := 0; i < len(pbs.PriorityRatios); i++ {
		pbs.roundRobinJobIndex[i] = 0
	}

	assignByPriorityCnts := make(map[int]int) // map to track the number of assignments by priority
	for i := 0; i < idleNodeCnt; i++ {
		p := pbs.getPriorityToAssign(i, idleNodeCnt)
		task, taskPriority := pbs.getNextTask(p, pbs.jobsByPriority, selectedTasks, i, idleNodeCnt)
		if task != nil {
			tasksToAssign = append(tasksToAssign, task)
			if _, ok := assignByPriorityCnts[taskPriority]; ok {
				assignByPriorityCnts[taskPriority] = assignByPriorityCnts[taskPriority] + 1
			} else {
				assignByPriorityCnts[taskPriority] = 1
			}
		} else if i > pbs.numBarredFromLowPriority {
			// task will = nil if only lowest priority tasks are waiting and we
			// are within the number of nodes reserved for non-lowest priority.
			// so only break when we didn't get a task and we are outside the
			// idle nodes reserved for non-lowest priority tasks
			break
		}
	}

	log.Infof("scheduling task (by priority):%v", assignByPriorityCnts)

	return tasksToAssign
}

/*
We are looking for the 'next' unscheduled task at the given priority.
The next task at a given priority level uses a round robin approach to get one
task from each of the jobs at that priority level.
If there are no unscheduled tasks at a given priority level, look for a task at the next
higher priority level.
If there are no higher level priority tasks, then start looking for a lower level priority task
params:
priority : we are trying to fill a task at this priority level
jobs: the list of jobs at this priority level
currentSelectedTasks: tasks already selected for scheduling
*/
func (pbs *PriorityBasedAlg) getNextTask(priority int, jobsByPriority [][]*jobState,
	currentSelectedTasks map[string]*taskState, idleNodeIdx, totalIdleNodes int) (*taskState, int) {
	// find the next job at priority whose turn it is to have a task scheduled
	origPriority := priority

	// find an unscheduled task at priority
	for priority >= 0 {
		task := pbs.getTaskFromRRJobs(priority, currentSelectedTasks)

		if task != nil {
			return task, priority
		}
		// there are no more jobs at this priority level with a waiting task
		// go to the 'next' priority level
		priority = pbs.getNextPriority(priority, origPriority, idleNodeIdx, totalIdleNodes)
	}
	return nil, -1
}

/*
get the next task using a round robin approach to selecting tasks from jobs a the given priority level
 */
func (pbs *PriorityBasedAlg) getTaskFromRRJobs(priority int, currentSelectedTasks map[string]*taskState) *taskState {

	if len(pbs.jobsByPriority[priority]) == 0 {
		return nil
	}

	rrIdx := pbs.roundRobinJobIndex[priority]
	if rrIdx >= len(pbs.jobsByPriority[priority]) {
		log.Fatalf("debugging breakpoint line, rrIdx is too big")
	}

	origRRIdx := rrIdx
	for true {
		task := pbs.getUnscheduledTaskInJob(pbs.jobsByPriority[priority][rrIdx], currentSelectedTasks)
		// update the rr index to next job (for next call)
		rrIdx++
		if rrIdx == len(pbs.jobsByPriority[priority]) {
			rrIdx = 0
		}
		if rrIdx == origRRIdx {
			return task
		}
		pbs.roundRobinJobIndex[priority] = rrIdx
		if rrIdx >= len(pbs.jobsByPriority[priority]) {
			log.Fatalf("debugging breakpoint line, rrIdx is too big")
		}

		if task != nil {
			return task
		}
	}
	return nil
}

/*
get a task that has not started, add it to currentSelectedTasks so it won't be
selected again in this iteration of GetTasksToBeAssigned()
*/
func (pbs *PriorityBasedAlg) getUnscheduledTaskInJob(job *jobState,
	currentSelectedTasks map[string]*taskState) *taskState {

	// get the next not started task in the job
	for taskId, task := range job.NotStarted {
		key := fmt.Sprintf("%stask%s", job.Job.Id, taskId)
		if _, ok := currentSelectedTasks[key]; ok {
			continue // the task is already selected
		}
		currentSelectedTasks[key] = task
		return task
	}
	return nil
}

/*
get the next priority level to assign... work from origPriority down to 0, then
from origPriority to last priority value
*/
func (pbs *PriorityBasedAlg) getNextPriority(currentPriority, origPriority,
	idleNodeIdx, totalIdleNodes int) int {
	var nextPriority int
	if currentPriority < origPriority {
		nextPriority = currentPriority - 1 // get next higher priority
	} else {
		nextPriority = currentPriority + 1 // get next lower priority
	}
	if nextPriority < 0 {
		nextPriority = origPriority + 1
	}

	withinReservedNodes := (totalIdleNodes - idleNodeIdx) < pbs.numBarredFromLowPriority
	if withinReservedNodes && nextPriority == len(pbs.PriorityRatios)-1 {
		return -1
	}

	if nextPriority < len(pbs.PriorityRatios) {
		return nextPriority
	}

	return -1
}

// track the index for the next job at the given priority level whose task should be selected
// use a round robin approach
func (pbs *PriorityBasedAlg) roundRobinJobIndexUpdate(priority int, jobs []*jobState) int {
	idx := pbs.roundRobinJobIndex[priority]
	idx++
	if idx == len(jobs) {
		if len(jobs) == 0 {
			idx = -1
		}
		idx = 0
	}
	pbs.roundRobinJobIndex[priority] = idx
	return pbs.roundRobinJobIndex[priority]
}

// find the lowest priority whose runRatio is a factor of the taskCounter
func (pbs *PriorityBasedAlg) getPriorityToAssign(idleNodeIdx, totalIdleNodes int) int {
	nextPriority := -1

	withinReservedNodes := (totalIdleNodes - idleNodeIdx) < pbs.numBarredFromLowPriority
	if pbs.taskModCounter == 1 {
		nextPriority = 0
	} else {
		for i := len(pbs.PriorityRatios) - 1; i >= 0 && nextPriority == -1; i-- {
			if math.Mod(float64(pbs.taskModCounter), float64(pbs.PriorityRatios[i])) == 0 &&
				!(withinReservedNodes && i == len(pbs.PriorityRatios)-1) {
				nextPriority = i
			}
		}
	}
	pbs.taskModCounter++
	if pbs.taskModCounter > pbs.PriorityRatios[len(pbs.PriorityRatios)-1] {
		pbs.taskModCounter = 1
	}
	return nextPriority
}
