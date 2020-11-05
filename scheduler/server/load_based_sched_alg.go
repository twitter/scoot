package server

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/stats"
)

const (
	under int = iota
	over
)

// defaults for the LoadBasedScheduler algorithm: only one class and all jobs map to that class
var DefaultLoadBasedSchedulerClassPcts = map[string]int32{"c0": 100}
var DefaultRequestorToClassMap = map[string]string{".*": "c0"}
var DefaultMinRebalanceTime = time.Duration(4 * time.Minute)
var MaxTaskDuration = time.Duration(4 * time.Hour)

type LoadBasedAlgConfig struct {
	classLoadPcts           map[string]int
	classLoadPctsMu         sync.RWMutex
	requestorReToClassMap   map[string]string
	requestorReToClassMapMU sync.RWMutex

	minRebalanceTime time.Duration // minimum time that must pass between rebalancing workers

	classByDescLoadPct []string

	stat stats.StatsReceiver
}

// LoadBasedAlg the scheduling algorithm allocates job tasks to workers using a class map as
// follows:
// - each job maps to a 'class' (based on the job's requestor value)
// - classes are assigned a % of the number of scoot workers.
// When the algorithm is assigning tasks to workers it will try to start tasks such that the
// number of running tasks maintain the defined class %'s.  We refer to number of workers as
// per a class's defined % as the number of 'entitled' workers for the class.
// (The class is entitled to use class % * total number of workers to run job tasks from jobs
// assigned to the class.)
// When there are not enough tasks to use all of the workers allotted to a class, the algorithm
// will allow other classes to run their tasks on the unused number of workers.  We refer to
// this as loaning worker counts to another class.
// Note: workers are not assigned to specific classes.  The class % concept is simply a counting
// mechanism.
//
// Each scheduling iteration tries to bring the task allocation back to the original class %
// assignment.  But, it could be the case that long running tasks slowly create an imbalance in
// the worker to class numbers.  As such, the algorithm periodically rebalances the running
// workers back toward the original target %s by canceling tasks that have been started on
// loaned workers.  It will cancel the most recently started tasks till the running task to class
// allocation meets the original targets.
//
type LoadBasedAlg struct {
	config     LoadBasedAlgConfig
	jobClasses map[string]*jobClass

	totalUnusedEntitlement int
	lastRebalanceTime      time.Time

	// local copy of load pcts and requestor map to use during assignment computation
	// to insulate the computation from external changes to the configuration
	classLoadPcts         map[string]int
	requestorReToClassMap map[string]string

	classByDescLoadPct             []string
	tasksByJobClassAndStartTimeSec map[string]map[time.Time]map[string]*taskState
}

// NewLoadBasedAlg allocate a new LoadBaseSchedAlg object.  If the load %'s don't add up to 100
// the %'s will be adjusted and an error will be returned with the alg object
func NewLoadBasedAlg(config LoadBasedAlgConfig, tasksByJobClassAndStartTimeSec map[string]map[time.Time]map[string]*taskState) *LoadBasedAlg {
	lbs := &LoadBasedAlg{
		config:                         config,
		jobClasses:                     map[string]*jobClass{},
		lastRebalanceTime:              time.Now(),
		tasksByJobClassAndStartTimeSec: tasksByJobClassAndStartTimeSec,
	}
	return lbs
}

// jobWaitingTaskIds map waiting task ids to the job state objects
type jobWaitingTaskIds struct {
	jobState       *jobState
	waitingTaskIDs []string
}

// jobClass the class definition that will be assigned to a set of jobs (using the job's requestor value)
type jobClass struct {
	className string

	// jobsByNumRunningTasks is a map that bins jobs by their number of running tasks.  Given that the algorithm has
	// determined it will start n tasks from class A, the tasks selected for starting from class A will give prefence
	// to jobs with the least number of running tasks.
	jobsByNumRunningTasks map[int][]jobWaitingTaskIds
	// the largest key value in the jobsByNumRunningTasks map
	maxTaskRunningMapIndex int

	origNumWaitingTasks    int
	origNumRunningTasks    int
	origTargetLoadPct      int // the target % of workers for this class
	origNumTargetedWorkers int // the original number of workers allocated for this class by target % (total workers * origTargetLoadPct)

	numTasksToStart int // number of tasks that can be started (when negative -> number of tasks to stop)
	numWaitingTasks int // number of tasks still waiting to be started

	tempEntitlement   int // temporary field to hold intermediate entitled num workers
	tempNormalizedPct int // temporary field to hold the normalized load %

}

func (jc *jobClass) String() string {
	return fmt.Sprintf("%s:TargetLoadPct:%d, origTasksWaiting:%d, origTasksRunning:%d, origTargetWorkers:%d, TasksToStart:%d, remainingWaitingTasks:%d, tempEntitlement:%d, tempNormalizedPct:%d",
		jc.className,
		jc.origTargetLoadPct,
		jc.origNumWaitingTasks,
		jc.origNumRunningTasks,
		jc.origNumTargetedWorkers,
		jc.numTasksToStart,
		jc.numWaitingTasks,
		jc.tempEntitlement,
		jc.tempNormalizedPct)
}

// NewJobClass a job class with its target % worker load
func NewJobClass(name string, targetLoadPct int) *jobClass {
	return &jobClass{className: name, origTargetLoadPct: targetLoadPct, jobsByNumRunningTasks: map[int][]jobWaitingTaskIds{}}
}

// GetTasksToBeAssigned - the entry point to the load based scheduling algorithm
// It returns the list of tasks that should be assigned to nodes as per the worker-class %s:
// Allocate available workers to classes based on target % allocations for each class and the current and
// running load for each class.
// - When a class has tasks waiting to start, the algorithm will determing the number of workers the class it 'entitled' to:
// the number of workers as per the class's target load %
// - When classes are under-utilizing their 'entitlement', (due to lack of waiting tasks), the unallocated workers will be
// ‘loaned’/used to run tasks from other classes (class allocations may exceed the targeted allocations %’s)
// -The algorithm will try to allocate 100% of the workers (no unallocated reserves)
//
// When starting tasks within a class:
// Jobs within a class are binned by the number of running tasks (ranking jobs by the number of active tasks).
// When starting tasks for a class, the tasks are first pulled from jobs with the least number of active tasks.
func (lbs *LoadBasedAlg) GetTasksToBeAssigned(jobsNotUsed []*jobState, stat stats.StatsReceiver, cs *clusterState,
	jobsByRequestor map[string][]*jobState) ([]*taskState, []*taskState) {
	log.Debugf("in LoadBasedAlg.GetTasksToBeAssigned: numWorkers:%d, numIdleWorkers:%d", len(cs.nodes), cs.numFree())

	// make local copies of the load pct structures
	lbs.classLoadPcts = lbs.LocalCopyClassLoadPcts()
	lbs.requestorReToClassMap = lbs.GetRequestorToClassMap()
	lbs.classByDescLoadPct = lbs.GetClassByDescLoadPct()

	numWorkers := len(cs.nodes)
	lbs.initOrigNumTargetedWorkers(numWorkers)

	lbs.initJobClassesMap(jobsByRequestor)
	lbs.ppJobClasses()

	// currentPctSpread is the delta between the highest and lowest
	currentPctSpread := lbs.getCurrentPctsSpread(numWorkers)
	var stopTasks []*taskState
	if currentPctSpread > 50 && time.Now().Sub(lbs.lastRebalanceTime) > lbs.config.minRebalanceTime {
		stopTasks = lbs.rebalanceClassTasks(jobsByRequestor, numWorkers, cs.numFree())
	} else {
		// compute the number of tasks to be started for each class
		lbs.computeNumTasksToStart(cs.numFree())
	}

	// add the tasks to be started to the return list
	tasksToStart := lbs.buildTaskStartList()

	// record the assignment stats
	for _, jc := range lbs.jobClasses {
		stat.Gauge(fmt.Sprintf("%s_%s", stats.SchedJobClassTasksStarting, jc.className)).Update(int64(jc.numTasksToStart))
		stat.Gauge(fmt.Sprintf("%s_%s", stats.SchedJobClassTasksWaiting, jc.className)).Update(int64(jc.origNumWaitingTasks - jc.numTasksToStart))
		stat.Gauge(fmt.Sprintf("%s_%s", stats.SchedJobClassTasksRunning, jc.className)).Update(int64(jc.origNumRunningTasks))
		stat.Gauge(fmt.Sprintf("%s_%s", stats.SchedJobClassDefinedPct, jc.className)).Update(int64(jc.origTargetLoadPct))
		finalPct := int(math.Round(float64(jc.origNumRunningTasks+jc.numTasksToStart) / float64(int64(numWorkers)*100.0)))
		stat.Gauge(fmt.Sprintf("%s_%s", stats.SchedJobClassActualPct, jc.className)).Update(int64(finalPct))
	}

	log.Debugf("Returning %d start tasks, %d stop tasks", len(tasksToStart), len(stopTasks))
	return tasksToStart, stopTasks
}

// initOrigNumTargetedWorkers computes the number of workers targeted for each class as per the class's
// original target load pct
func (lbs *LoadBasedAlg) initOrigNumTargetedWorkers(numWorkers int) {
	lbs.jobClasses = map[string]*jobClass{}
	totalWorkers := 0
	firstClass := true
	for _, className := range lbs.classByDescLoadPct {
		jc := &jobClass{className: className, origTargetLoadPct: lbs.classLoadPcts[className], jobsByNumRunningTasks: map[int][]jobWaitingTaskIds{}}
		lbs.jobClasses[className] = jc
		if firstClass {
			firstClass = false
			continue
		}
		targetNumWorkers := int(math.Floor(float64(numWorkers) * float64(jc.origTargetLoadPct) / 100.0))
		jc.origNumTargetedWorkers = targetNumWorkers
		totalWorkers += targetNumWorkers
	}
	lbs.jobClasses[lbs.classByDescLoadPct[0]].origNumTargetedWorkers = numWorkers - totalWorkers
}

// initJobClassesMap builds the map of requestor (class name) to jobClass objects
// if we see a job whose class % not defined, assign the job to the class with the
// least number of workers
func (lbs *LoadBasedAlg) initJobClassesMap(jobsByRequestor map[string][]*jobState) {
	classNameWithLeastWorkers := lbs.classByDescLoadPct[len(lbs.classByDescLoadPct)-1]
	// fill the jobClasses map with the state of the running jobs
	for requestor, jobs := range jobsByRequestor {
		var jc *jobClass
		var ok bool
		className := GetRequestorClass(requestor, lbs.requestorReToClassMap)
		if className != "" {
			jc, ok = lbs.jobClasses[className]
			if !ok {
				// the class name was not recognized, use the lowest priority class
				lbs.config.stat.Counter(stats.SchedLBSUnknownJobCnt).Inc(1)
				jc = lbs.jobClasses[classNameWithLeastWorkers]
				log.Errorf("%s is not a recognized job class assigning to lowest priority class (%s)", className, classNameWithLeastWorkers)
			}
		} else {
			// if the requestor is not recognized, use the lowest priority class
			lbs.config.stat.Counter(stats.SchedLBSUnknownJobCnt).Inc(1)
			jc = lbs.jobClasses[classNameWithLeastWorkers]
			log.Errorf("%s is not a recognized requestor assigning to lowest priority class (%s)", className, classNameWithLeastWorkers)
		}
		if jc.origTargetLoadPct == 0 {
			log.Errorf("%s worker allocation (load %% is 0), ignoring %d jobs", requestor, len(jobs))
			lbs.config.stat.Counter(stats.SchedLBSIgnoredJobCnt).Inc(1)
			continue
		}

		// organize the class's jobs by the number of tasks currently running (map of jobs indexed by the number of
		// tasks currently running for the job).  This will be used in the round robin task selection to start a
		// class's worker allocation at the jobs with least number of running tasks
		// this loop also computes the class's running tasks and waiting task totals
		for _, job := range jobs {
			_, ok := jc.jobsByNumRunningTasks[job.TasksRunning]
			if !ok {
				jc.jobsByNumRunningTasks[job.TasksRunning] = []jobWaitingTaskIds{}
			}
			waitingTaskIds := []string{}
			for taskID := range job.NotStarted {
				waitingTaskIds = append(waitingTaskIds, taskID)
			}
			jc.jobsByNumRunningTasks[job.TasksRunning] = append(jc.jobsByNumRunningTasks[job.TasksRunning], jobWaitingTaskIds{jobState: job, waitingTaskIDs: waitingTaskIds})
			if job.TasksRunning > jc.maxTaskRunningMapIndex {
				jc.maxTaskRunningMapIndex = job.TasksRunning
			}
			jc.origNumRunningTasks += job.TasksRunning
			jc.origNumWaitingTasks += len(job.NotStarted)
		}

		jc.numWaitingTasks = jc.origNumWaitingTasks
	}
}

// GetRequestorClass find the requestorToClass entry for requestor
// keys in requestorToClassEntry are regular expressions
// if no match is found, return "" for the class name
func GetRequestorClass(requestor string, requestorToClassMap map[string]string) string {
	for reqRe, className := range requestorToClassMap {
		if m, _ := regexp.Match(reqRe, []byte(requestor)); m {
			return className
		}
	}
	return ""
}

// computeNumTasksToStart - computes the the number of tasks to start for each class.
// Perform the entitlement calculation first and if there are still unallocated wokers
// and tasks waiting to start, compute the loan calculation.
func (lbs *LoadBasedAlg) computeNumTasksToStart(numIdleWorkers int) {

	var haveUnallocatedTasks bool

	numIdleWorkers, haveUnallocatedTasks = lbs.entitlementTasksToStart(numIdleWorkers)

	if numIdleWorkers > 0 && haveUnallocatedTasks {
		lbs.workerLoanAllocation(numIdleWorkers)
	}
}

// entitlementTasksToStart compute the number of tasks we can start for each class based on each classes original targeted
// number of workers (origNumTargetedWorkers)
// Note: this is an iterative computation that converges on the number of tasks to start within number of class's iterations.
//
// 1. compute the entitlement of a class as the class's orig target load minus (number of tasks running + number of tasks that
// can be started)  (exception: if a class does not have waiting tasks, its entitlement is 0)
// 2. compute entitlement % as entitlement/total of all classes entitlements
// 3. compute num tasks to start for each class as min(entitlement % * idle(unallocated) workers, number of the class's waiting tasks)
//
// After completing the 3 steps above, the sum of the number tasks to start may still be < number of idle workers.  This will happen
// when a class's waiting task count < than its entitlement (the class is not using all of its entitlement).  When this happens,
// the un-allocated idle workers can be distributed across the other classes that have waiting tasks and have not met their full
// entitlement.  We compute this by repeating steps 1-3 till all idle workers have been allocated, all waiting tasks have been
// allocated or all classes entitlements have been met.  Each iteration either uses up all idle workers, all of a class's waiting tasks
// or fully allocates at least one class's task entitlement.   This means that the we will not iterate more than the number of classes.
func (lbs *LoadBasedAlg) entitlementTasksToStart(numIdleWorkers int) (int, bool) {
	i := 0
	haveWaitingTasks := true
	for ; i < len(lbs.jobClasses); i++ {
		// compute the class's current entitlement: number of tasks we would like to start for each class as per the class's
		// target load % and number of waiting tasks.  We'll use this to compute normalized entitlement %s below.
		totalEntitlements := 0
		// get the current entitlements
		for _, jc := range lbs.jobClasses {
			if (jc.origNumRunningTasks+jc.numTasksToStart) <= jc.origNumTargetedWorkers && jc.numWaitingTasks > 0 {
				jc.tempEntitlement = jc.origNumTargetedWorkers - (jc.origNumRunningTasks + jc.numTasksToStart)
			} else {
				jc.tempEntitlement = 0
			}
			totalEntitlements += jc.tempEntitlement
		}

		if totalEntitlements == 0 {
			// the class's task allocations have used up each class's entitlement, break
			// so we can move on to calculating loaned workers
			log.Infof("no more entitlements to allocate")
			break
		}

		// compute normalized entitlement pcts for classes with entitlement > 0
		lbs.computeEntitlementPcts()

		// compute worker allocations as per the normalized entitlement %s
		numTasksAllocated := 0
		workersToAllocate := min(numIdleWorkers, totalEntitlements)
		numTasksAllocated, haveWaitingTasks = lbs.getTaskAllocations(workersToAllocate)

		numIdleWorkers -= numTasksAllocated

		if !haveWaitingTasks {
			break
		}
		if numIdleWorkers <= 0 {
			break
		}
	}
	return numIdleWorkers, haveWaitingTasks
}

// loanWorkers: We have workers that can be 'loaned' to classes that still have waiting tasks.
// Note: this is an iterative computation that will converge on the number of workers to loan to classes
// For each iteration
// 1. normalize the original target load % to those classes with waiting tasks
// 2. compute each class's allowed loan amount as the number of unallocated workers * the normalized % but not to
// exceed the class's number of waiting tasks
//
// When a class's allowed loan amount is larger than the class's waiting tasks, there will be unallocated workers
// after all the class 'loan' amounts have been calculated.  We distribute these unallocated workers by
// repeating the loan calculation till there are no unallocated workers left.
// Each iteration either uses up all idle workers, or all of a class's waiting tasks.  This means that the we will not
// iterate more than the number of classes.
func (lbs *LoadBasedAlg) workerLoanAllocation(numIdleWorkers int) {
	i := 0
	for ; i < len(lbs.jobClasses); i++ {
		lbs.computeLoanPcts()

		// compute loan %'s and allocate idle workers
		numTasksAllocated, haveWaitingTasks := lbs.getTaskAllocations(numIdleWorkers)

		numIdleWorkers -= numTasksAllocated

		if !haveWaitingTasks {
			break
		}
		if numIdleWorkers <= 0 {
			break
		}
	}
	lbs.ppAllocationState(fmt.Sprintf("ended loan allocation after %d loops", i), numIdleWorkers)
}

// getTaskAllocations given the normalized allocation %s for each class, working from highest % (largest allocation) to smallest,
// allocate that class's % of the idle workers (update the class's numTasksToStart and numWaitingTasks), but not to exceed the
// classs' number of waiting tasks. Return the total number of tasks allocated to workers and a boolean indicating if there are
// still tasks waiting to be allocated
func (lbs *LoadBasedAlg) getTaskAllocations(numIdleWorkers int) (int, bool) {
	totalTasksAllocated := 0
	haveWaitingTasks := false

	for _, className := range lbs.classByDescLoadPct {
		jc := lbs.jobClasses[className]
		numTasksToStart := min(jc.numWaitingTasks, ceil(float32(numIdleWorkers)*(float32(jc.tempNormalizedPct)/100.0)))

		if (totalTasksAllocated + numTasksToStart) > numIdleWorkers {
			numTasksToStart = numIdleWorkers - totalTasksAllocated
		}
		jc.numTasksToStart += numTasksToStart
		jc.numWaitingTasks -= numTasksToStart
		if jc.numWaitingTasks > 0 {
			haveWaitingTasks = true
		}
		totalTasksAllocated += numTasksToStart
	}
	return totalTasksAllocated, haveWaitingTasks
}

// computeEntitlementPcts computes each class's current entitled % of total entitlements (from the current)
// entitlement values
func (lbs *LoadBasedAlg) computeEntitlementPcts() {
	// get the entitlements total
	entitlementTotal := 0
	for _, jc := range lbs.jobClasses {
		entitlementTotal += jc.tempEntitlement
	}

	// compute the % for all but the highest priority class.  Add up all computed %s and assign
	// 100 - sum of % to the highest priority class (this eliminates rounding errors, forcing the
	// % to add up to 100%)
	totalPcts := 0
	firstClass := true
	for _, className := range lbs.classByDescLoadPct {
		if firstClass {
			firstClass = false
			continue
		}
		jc := lbs.jobClasses[className]
		jc.tempNormalizedPct = int(math.Floor(float64(jc.tempEntitlement) * 100.0 / float64(entitlementTotal)))
		totalPcts += jc.tempNormalizedPct
	}
	lbs.jobClasses[lbs.classByDescLoadPct[0]].tempNormalizedPct = 100 - totalPcts
}

// computeLoanPcts as orig load %'s normalized to exclude classes that don't have waiting tasks
func (lbs *LoadBasedAlg) computeLoanPcts() {
	// get the sum of all the original load pcts for classes that have waiting tasks
	pctsTotal := 0
	for _, jc := range lbs.jobClasses {
		if jc.numWaitingTasks > 0 {
			pctsTotal += jc.origTargetLoadPct
		}
	}

	if pctsTotal == 0 {
		return
	}

	// compute the % for all but the highest priority class.  Add up all computed %s and assign
	// 100 - sum of % to the highest class from the range (this eliminates rounding errors, forcing the
	// sum or % to go to 100%)
	totalPcts := 0
	firstClass := true
	firstClassName := ""
	for _, className := range lbs.classByDescLoadPct {
		jc := lbs.jobClasses[className]
		if jc.numWaitingTasks > 0 {
			if firstClass {
				firstClass = false
				firstClassName = className
				continue
			}
			jc.tempNormalizedPct = int(math.Floor(float64(jc.origTargetLoadPct) * 100.0 / float64(pctsTotal)))
			totalPcts += jc.tempNormalizedPct
		} else {
			jc.tempNormalizedPct = 0
		}
	}
	lbs.jobClasses[firstClassName].tempNormalizedPct = 100 - totalPcts
}

// buildTaskStartList builds the list of tasks to be started for each jobClass.
func (lbs *LoadBasedAlg) buildTaskStartList() []*taskState {
	tasks := []*taskState{}
	for _, jc := range lbs.jobClasses {
		if jc.numTasksToStart <= 0 {
			continue
		}
		classTasks := lbs.getTasksToStartForJobClass(jc)
		tasks = append(tasks, classTasks...)
	}
	return tasks
}

// getTasksToStartForJobClass get the tasks to start list for a given jobClass.  The jobClass's numTasksToStart
// field will contain the number of tasks to start for this job class.  The jobClass's jobsByNumRunningTasks is
// a map from an integer value (number of tasks running) to the list of jobs with that number of tasks running
// For a given jobClass, we start adding tasks from the jobs with the least number of tasks running.
// (Note: when a task is started for a job, the job is moved to the ‘next’ bin and placed at the end of that bin’s job list.)
func (lbs *LoadBasedAlg) getTasksToStartForJobClass(jc *jobClass) []*taskState {
	tasks := []*taskState{}

	startingTaskCnt := 0
	// work our way through the class's jobs, starting with jobs with the least number of running tasks,
	// till we've added the class's numTasksToStart number of tasks to the task list
	for numRunningTasks := 0; numRunningTasks <= jc.maxTaskRunningMapIndex; numRunningTasks++ {
		var jobs []jobWaitingTaskIds
		var ok bool
		if jobs, ok = jc.jobsByNumRunningTasks[numRunningTasks]; !ok {
			// there are no jobs with numRunningTasks running tasks, move on to jobs with more running tasks
			continue
		}
		// jobs contains list of jobs and their waiting taskIds. (Each job in this list has the same number of running tasks.)
		// Allocate one task from each job till we've allocated numTasksToStart for the jobClass, or have allocated 1 task from
		// each job in this list.  As we allocate a task for a job, move the job to the end of jc.jobsByNumRunningTasks[numRunningTasks+1].
		for _, job := range jobs {
			if job.waitingTaskIDs != nil && len(job.waitingTaskIDs) > 0 {
				tasks = append(tasks, job.jobState.NotStarted[job.waitingTaskIDs[0]])

				if len(job.waitingTaskIDs) > 1 {
					job.waitingTaskIDs = job.waitingTaskIDs[1:]
					jc.jobsByNumRunningTasks[numRunningTasks+1] = append(jc.jobsByNumRunningTasks[numRunningTasks+1], job)
					if numRunningTasks == jc.maxTaskRunningMapIndex {
						jc.maxTaskRunningMapIndex++
					}
				}

				startingTaskCnt++
				if startingTaskCnt == jc.numTasksToStart {
					return tasks
				}
			}

		}
	}

	return tasks // note: we should never hit this line
}

// buildTaskStopList builds the list of tasks to be stopped.
func (lbs *LoadBasedAlg) buildTaskStopList() []*taskState {
	tasks := []*taskState{}
	for _, jc := range lbs.jobClasses {
		if jc.numTasksToStart >= 0 {
			continue
		}
		classTasks := lbs.getTasksToStopForJobClass(jc)
		tasks = append(tasks, classTasks...)
	}
	return tasks
}

// getTasksToStopForJobClass for each job class return the abs(numTasksToStart) most recently started
// tasks.  (numTasksToStart will be a negative number)
func (lbs *LoadBasedAlg) getTasksToStopForJobClass(jobClass *jobClass) []*taskState {
	earliest := time.Now().Add(-1 * MaxTaskDuration)
	startTimeSec := time.Now().Truncate(time.Second)
	numTasksToStop := jobClass.numTasksToStart * -1
	stopTasks := []*taskState{}
	for len(stopTasks) < numTasksToStop {
		tasks := GetTasksByJobClassAndStart(lbs.tasksByJobClassAndStartTimeSec, jobClass.className, startTimeSec)
		for _, task := range tasks {
			stopTasks = append(stopTasks, task)
			if len(stopTasks) == numTasksToStop {
				break
			}
		}
		startTimeSec = startTimeSec.Add(-1 * time.Second).Truncate(time.Second)
		if startTimeSec.Before(earliest) {
			break
		}
	}
	return stopTasks
}

func (lbs *LoadBasedAlg) getNumTasksToStart(requestor string) int {
	return lbs.jobClasses[requestor].numTasksToStart
}

// ppAllocationState pretty print the jobClasses
func (lbs *LoadBasedAlg) ppAllocationState(tag string, unallocatedWorkers int) {
	log.Debugf("*******%s, %d workers left to be allocated", tag, unallocatedWorkers)
	for _, className := range lbs.classByDescLoadPct {
		jc := lbs.jobClasses[className]
		log.Debugf("%s", jc)
	}
}

// ppJBR pretty print the jobsByRequestor map
func (lbs *LoadBasedAlg) ppJBR(jbr map[string][]*jobState) {
	for k, v := range jbr {
		log.Infof("requestor: %s, %d jobs", k, len(v))
	}
}

// rebalanceClassTasks compute the tasks that should be deleted to allow the scheduling algorithm
// to start tasks in better alignment with the original targeted task load percents.
// The function returns the list of tasks to stop and updates the jobClass objects with the number
// of tasks to start
func (lbs *LoadBasedAlg) rebalanceClassTasks(jobsByRequestor map[string][]*jobState, totalWorkers int, numIdleWorkers int) []*taskState {

	totalTasks := 0
	// compute number tasks as per the each class's entitlement and waiting tasks
	// will be negative when a class is over its entitlement
	for _, jc := range lbs.jobClasses {
		if jc.origNumRunningTasks > jc.origNumTargetedWorkers {
			// the number of tasks that could be started to bring the class up to its entitlement
			jc.numTasksToStart = jc.origNumTargetedWorkers - jc.origNumRunningTasks
		} else if jc.origNumRunningTasks+jc.origNumWaitingTasks < jc.origNumTargetedWorkers {
			// the waiting tasks won't put the class over its entitlement
			jc.numTasksToStart = jc.origNumWaitingTasks
		} else {
			// the class is running more than its entitled workers, numTasksToStart is number of tasks
			// to stop to bring back to its entitlement (it will be a negative number)
			jc.numTasksToStart = jc.origNumTargetedWorkers - jc.origNumRunningTasks
		}
		jc.numWaitingTasks = jc.origNumWaitingTasks - jc.numTasksToStart
		totalTasks += jc.origNumRunningTasks + jc.numTasksToStart
	}

	if totalTasks < totalWorkers {
		// some classes are not using their full allocation, we can loan workers
		lbs.computeLoanPcts()

		lbs.getTaskAllocations(totalWorkers - totalTasks)
	}

	stopTasks := lbs.buildTaskStopList()

	return stopTasks
}

// getCurrentPctsSpread is used to measure how well the current worker assignment matches the target loads.
// It computes each class's difference between the target load pct and actual load pct.  The 'spread' value
// is the difference between the min and max differences across the classes.  Eg: if 30% was the load target
// for class A and class A is using 50% of the workers, A's pct difference is -20%.  If class B has a target
// of 15% and is only using 5% of workers, B's pct difference is 10%.  If A and B are the only classes, the
// pctSpread is 25% (5% - -20%).
func (lbs *LoadBasedAlg) getCurrentPctsSpread(totalWorkers int) int {
	if len(lbs.jobClasses) < 2 {
		return 0
	}
	minPct := 0
	maxPct := 0
	for _, className := range lbs.classByDescLoadPct {
		jc := lbs.jobClasses[className]
		currPct := int(math.Floor(float64(jc.origNumRunningTasks) * 100.0 / float64(totalWorkers)))
		pctDiff := jc.origTargetLoadPct - currPct
		minPct = min(minPct, pctDiff)
		maxPct = max(maxPct, pctDiff)
	}

	return maxPct - minPct
}

// GetClassByDescLoadPct get a copy of the config's class by descending load pcts
func (lbs *LoadBasedAlg) GetClassByDescLoadPct() []string {
	lbs.config.classLoadPctsMu.RLock()
	defer lbs.config.classLoadPctsMu.RUnlock()
	copy := []string{}
	for _, v := range lbs.config.classByDescLoadPct {
		copy = append(copy, v)
	}
	return copy
}

// GetClassLoadPcts return a copy of the ClassLoadPcts converting to int32
func (lbs *LoadBasedAlg) GetClassLoadPcts() map[string]int32 {
	lbs.config.classLoadPctsMu.RLock()
	defer lbs.config.classLoadPctsMu.RUnlock()
	copy := map[string]int32{}
	for k, v := range lbs.config.classLoadPcts {
		copy[k] = int32(v)
	}
	return copy
}

// LocalCopyClassLoadPcts return a copy of the ClassLoadPcts leaving as int
func (lbs *LoadBasedAlg) LocalCopyClassLoadPcts() map[string]int {
	lbs.config.classLoadPctsMu.RLock()
	defer lbs.config.classLoadPctsMu.RUnlock()
	copy := map[string]int{}
	for k, v := range lbs.config.classLoadPcts {
		copy[k] = v
	}
	return copy
}

// SetClassLoadPcts set the scheduler's class load pcts with a copy of the input class load pcts
func (lbs *LoadBasedAlg) SetClassLoadPcts(classLoadPcts map[string]int32) {
	lbs.config.classLoadPctsMu.Lock()
	defer lbs.config.classLoadPctsMu.Unlock()

	// build a list that orders the classes by descending pct.
	keys := []string{}
	for key := range classLoadPcts {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return classLoadPcts[keys[i]] > classLoadPcts[keys[j]]
	})
	lbs.config.classByDescLoadPct = keys

	// set the load pcts - normalizing them if the don't add up to 100
	lbs.config.classLoadPcts = map[string]int{}
	pctTotal := 0
	for _, val := range classLoadPcts {
		pctTotal += int(val)
	}
	if pctTotal != 100 {
		log.Errorf("LoadBalanced scheduling %%'s don't add up to 100, normalizing them")
		totalNormalizedPct := 0
		firstClass := true
		for _, className := range lbs.config.classByDescLoadPct {
			if firstClass {
				firstClass = false
				continue // skip the first class (highest %), it will be given the difference between 100 and the sum of the other %'s
			}
			classPct := classLoadPcts[className]
			lbs.config.classLoadPcts[className] = int(math.Floor(float64(classPct) / float64(pctTotal)))
			totalNormalizedPct += lbs.config.classLoadPcts[className]
		}
		lbs.config.classLoadPcts[lbs.config.classByDescLoadPct[0]] = 100 - totalNormalizedPct
		log.Errorf("LoadBalanced scheduling class percents have been changed to %v", lbs.config.classLoadPcts)
	} else {
		for k, v := range classLoadPcts {
			lbs.config.classLoadPcts[k] = int(v)
		}
	}
}

// GetRequestorToClassMap return a copy of the RequestorToClassMap
func (lbs *LoadBasedAlg) GetRequestorToClassMap() map[string]string {
	lbs.config.requestorReToClassMapMU.RLock()
	defer lbs.config.requestorReToClassMapMU.RUnlock()
	copy := map[string]string{}
	for k, v := range lbs.config.requestorReToClassMap {
		copy[k] = v
	}
	return copy
}

// SetRequestorToClassMap set the scheduler's requestor to class map with a copy of the input map
func (lbs *LoadBasedAlg) SetRequestorToClassMap(requestorToClassMap map[string]string) {
	lbs.config.requestorReToClassMapMU.Lock()
	defer lbs.config.requestorReToClassMapMU.Unlock()
	lbs.config.requestorReToClassMap = map[string]string{}
	for k, v := range requestorToClassMap {
		lbs.config.requestorReToClassMap[k] = v
	}
}

func (lbs *LoadBasedAlg) ppJobClasses() {
	log.Info("job class definitions:")
	for class, jc := range lbs.jobClasses {
		log.Infof("%s:%s", class, jc)
	}
}
