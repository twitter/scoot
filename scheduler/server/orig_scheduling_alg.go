package server

import (
	"math"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/scheduler/domain"
)

type OrigSchedulingAlg struct{}

func NewOrigSchedulingAlg() *OrigSchedulingAlg {
	return &OrigSchedulingAlg{}
}

func (osa *OrigSchedulingAlg) GetTasksToBeAssigned(jobs []*jobState, stat stats.StatsReceiver, cs *clusterState,
	requestors map[string][]*jobState, cfg SchedulerConfig) []*taskState {
	// Sort jobs by priority and count running tasks.
	// An array indexed by priority. The value is the subset of jobs in fifo order for the given priority.
	priorityJobs := [][]*jobState{{}, {}, {}}
	for _, job := range jobs {
		p := int(job.Job.Def.Priority)
		priorityJobs[p] = append(priorityJobs[p], job)
	}
	stat.Gauge(stats.SchedPriority0JobsGauge).Update(int64(len(priorityJobs[domain.P0])))
	stat.Gauge(stats.SchedPriority1JobsGauge).Update(int64(len(priorityJobs[domain.P1])))
	stat.Gauge(stats.SchedPriority2JobsGauge).Update(int64(len(priorityJobs[domain.P2])))
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
	remainingOptional := [][][]*taskState{{}, {}, {}}
	remainingRequired := [][][]*taskState{{}, {}, {}}
Loop:
	for _, p := range []domain.Priority{domain.P2, domain.P1, domain.P0} {
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
			numScaledTasks := math.Ceil(float64(numTasks) * float64(cfg.GetNodeScaleFactor(len(cs.nodes), p)))
			numSchedulable := 0
			numDesiredUnmet := 0

			// For this group of tasks we want the lesser of: the number remaining, or a number based on load.
			numDesired := math.Min(float64(len(unsched)), float64(numScaledTasks))
			// The number of tasks we can schedule is reduced by the number of tasks we're already running.
			// Further the number we'd like and the number we'll actually schedule are restricted by numAvailNodes.
			// If numDesiredUnmet==0 then any remaining outstanding tasks are low priority and appended to remainingOptional
			numDesiredUnmet = int(math.Max(0.0, numDesired-float64(numRunning+numAvailNodes)))
			numSchedulable = int(math.Min(float64(numAvailNodes), math.Max(0.0, numDesired-float64(numRunning))))

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
				numFromFree := int(math.Min(float64(numFree), float64(numSchedulable)))
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
	for j, quota := range [][]float64{NodeScaleAdjustment, {1, 1, 1}, NodeScaleAdjustment, {1, 1, 1}} {
		remaining := &remainingRequired
		if j > 1 {
			remaining = &remainingOptional
		}
		for _, p := range []domain.Priority{domain.P2, domain.P1, domain.P0} {
			if numFree == 0 {
				break LoopRemaining
			}
			// The remaining tasks, bucketed by job, for a given priority.
			taskLists := &(*remaining)[p]
			// Distribute the allowed number of free nodes evenly across the bucketed jobs for a given priority.
			nodeQuota := math.Ceil((float64(numFreeBasis) * quota[p]) / float64(len(*taskLists)))
			for i := 0; i < len(*taskLists); i++ {
				taskList := &(*taskLists)[i]
				// Noting that we use ceil() above, we may use less quota than assigned if it's unavailable or unneeded.
				nTasks := int(math.Min(math.Min(float64(numFree), nodeQuota), float64(len(*taskList))))
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
	return tasks
}
