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
	"github.com/twitter/scoot/scheduler/domain"
)

const (
	under int = iota
	over
)

// defaults for the LoadBasedScheduler algorithm: only one class and all jobs map to that class
var (
	DefaultLoadBasedSchedulerClassPercents = map[string]int32{
		"land":       40,
		"diff":       25,
		"sandbox":    10,
		"regression": 17,
		"ktf":        3,
		"coverage":   2,
		"tryout":     2,
		"unknown":    1,
	}
	DefaultRequestorToClassMap = map[string]string{
		"land.*":       "land",
		"diff.*":       "diff",
		"sandbox.*":    "sandbox",
		"regression.*": "regression",
		"CI.*":         "regression",
		"jenkins.*":    "ktf",
		"ktf.*":        "ktf",
		"coverage.*":   "coverage",
		"tryout.*":     "tryout",
	}
	DefaultMinRebalanceTime = time.Duration(4 * time.Minute)
	MaxTaskDuration         = time.Duration(4 * time.Hour)
)

type LoadBasedAlgConfig struct {
	classLoadPercents       map[string]int
	classLoadPercentsMu     sync.RWMutex
	requestorReToClassMap   map[string]string
	requestorReToClassMapMU sync.RWMutex

	rebalanceThreshold     int
	rebalanceThresholdMu   sync.RWMutex
	rebalanceMinDuration   time.Duration
	rebalanceMinDurationMu sync.RWMutex

	classByDescLoadPct []string

	stat stats.StatsReceiver
}

// LoadBasedAlg the scheduling algorithm computes the list of tasks to start and stop for the
// current iteration of the scheduler loop.
//
// The algorithm uses the classLoadPercents, number of tasks currently running for each class,
// and number of available workers when computing the tasks to start/stop.
//
// the algorithm has 3 main phases: rebalancing, entitlement allocation, loaning allocation.
//
// - the rebalancing phase is only triggered when the number of tasks running for each class is very
// different than the class load percents.  When rebalancing is run, the rebalancing computation will
// select the set of tasks that need to be stopped to bring classes that are over their target load
// percents back to the target load percents, and the tasks that can be started to replace the stopped tasks.
// Note: there may still be idle workers after the rebalance tasks are started/stopped.  The next
// scheduling iteration will find tasks for these workers.
//
// the entitlement part of the computation identifies the number of tasks to start to bring the number
// of running tasks for each class closer to its entitlement as defined in the classLoadPercents.
//
// the loan part of the computation identifies the number of tasks over a class's entitlement that can
// be started to use up unused workers due to other classes not using their entitlement.
//
// See README.md for more details
type LoadBasedAlg struct {
	config     *LoadBasedAlgConfig
	jobClasses map[string]*jobClass

	totalUnusedEntitlement          int
	exceededRebalanceThresholdStart time.Time

	// local copy of load pcts and requestor map to use during assignment computation
	// to insulate the computation from external changes to the configuration
	classLoadPercents     map[string]int
	requestorReToClassMap map[string]string

	classByDescLoadPct             []string
	tasksByJobClassAndStartTimeSec map[taskClassAndStartKey]taskStateByJobIDTaskID

	nopAllocationsCnt int // count the number of times, we've run LBS without allocating any nodes to tasks
}

// NewLoadBasedAlg allocate a new LoadBaseSchedAlg object.
func NewLoadBasedAlg(config *LoadBasedAlgConfig, tasksByJobClassAndStartTimeSec map[taskClassAndStartKey]taskStateByJobIDTaskID) *LoadBasedAlg {
	lbs := &LoadBasedAlg{
		config:                          config,
		jobClasses:                      map[string]*jobClass{},
		exceededRebalanceThresholdStart: time.Time{},
		tasksByJobClassAndStartTimeSec:  tasksByJobClassAndStartTimeSec,
	}
	return lbs
}

// jobWaitingTasks waiting task ids (in the order they should be started) for a job
type jobWaitingTasks struct {
	jobState     *jobState
	waitingTasks []*taskState
}

// jobClass the structure used by the algorithm when computing the list of tasks to start/stop for each class.
// It tracks a class's load percent, and the current state of jobs/tasks assigned to the class.  It also holds
// the intermediate fields used to calculate the tasks to start/stop.
type jobClass struct {
	className string

	// jobsByNumRunningTasks is a map that bins jobs by their number of running tasks.  Given that the algorithm has
	// determined it will start n tasks from class A, the tasks selected for starting from class A will give prefence
	// to jobs with the least number of running tasks.
	jobsByNumRunningTasks map[int][]jobWaitingTasks
	// the largest key value in the jobsByNumRunningTasks map
	maxTaskRunningMapIndex int

	// the total number of tasks waiting to start for all jobs in this class
	origNumWaitingTasks int
	// the total number of tasks running for all jobs in this class
	origNumRunningTasks int

	// the target % of workers for this class
	origTargetLoadPct int
	// the original number of workers that should be running this class's tasks (total workers * origTargetLoadPct)
	origNumTargetedWorkers int
	// number of tasks that can be started (when negative -> number of tasks to stop)
	numTasksToStart int
	// number of tasks still waiting to be started
	numWaitingTasks int

	// temporary field to hold intermediate entitled values
	tempEntitlement int
	// temporary field to hold the normalized load %
	tempNormalizedPct int
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

// GetTasksToBeAssigned - the entry point to the load based scheduling algorithm
// The algorithm uses the classLoadPercents, number of tasks currently running for each class
// and number of available workers when computing the tasks to start/stop.
// The algorithm has 3 main phases: rebalancing, entitlement allocation, loaning allocation.
func (lbs *LoadBasedAlg) GetTasksToBeAssigned(jobsNotUsed []*jobState, stat stats.StatsReceiver, cs *clusterState,
	jobsByRequestor map[string][]*jobState) ([]*taskState, []*taskState) {

	// make local copies of the load pct structures to isolate the algorithm from user updates that may happen
	// as the algorithm is running
	lbs.classLoadPercents = lbs.LocalCopyClassLoadPercents()
	lbs.requestorReToClassMap = lbs.getRequestorToClassMap()
	lbs.classByDescLoadPct = lbs.getClassByDescLoadPct()

	numWorkers := len(cs.nodes)
	lbs.initOrigNumTargetedWorkers(numWorkers)

	lbs.initJobClassesMap(jobsByRequestor)

	rebalanced := false
	var tasksToStop []*taskState
	if lbs.getRebalanceMinimumDuration() > 0 && lbs.getRebalanceThreshold() > 0 {
		// currentPctSpread is the delta between the highest and lowest delta from target percent loads
		currentPctSpread := lbs.getCurrentPercentsSpread(numWorkers)
		if currentPctSpread > lbs.getRebalanceThreshold() {
			nilTime := time.Time{}
			if lbs.exceededRebalanceThresholdStart == nilTime {
				lbs.exceededRebalanceThresholdStart = time.Now()
			} else if time.Now().Sub(lbs.exceededRebalanceThresholdStart) > lbs.config.rebalanceMinDuration {
				tasksToStop = lbs.rebalanceClassTasks(jobsByRequestor, numWorkers)
				lbs.exceededRebalanceThresholdStart = time.Time{}
				rebalanced = true
			}
		}
	}

	if !rebalanced {
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

	// log the number of tasks to start/stop (when > 0)
	// (also track number of times we came into this code, but didn't return any tasks to start/stop)
	if len(tasksToStart) > 0 || len(tasksToStop) > 0 {
		log.Debugf("Returning %d start tasks, %d stop tasks, for %d free nodes out of %d total nodes.\n"+
			"(%d calls to GetTasksToBeAssigned() with no tasks assigned)\n",
			len(tasksToStart), len(tasksToStop), cs.numFree(), len(cs.nodes), lbs.nopAllocationsCnt)
		lbs.nopAllocationsCnt = 0
	} else {
		lbs.nopAllocationsCnt++
	}
	return tasksToStart, tasksToStop
}

// initOrigNumTargetedWorkers computes the number of workers targeted for each class as per the class's
// original target load pct
func (lbs *LoadBasedAlg) initOrigNumTargetedWorkers(numWorkers int) {
	lbs.jobClasses = map[string]*jobClass{}
	totalWorkers := 0
	firstClass := true
	for _, className := range lbs.classByDescLoadPct {
		jc := &jobClass{className: className, origTargetLoadPct: lbs.classLoadPercents[className], jobsByNumRunningTasks: map[int][]jobWaitingTasks{}}
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

// initJobClassesMap builds the map of waiting jobs' requestors to jobClass objects
// if we see a job whose class % not defined, assign the job to the class with the
// smallest class load %
func (lbs *LoadBasedAlg) initJobClassesMap(jobsByRequestor map[string][]*jobState) {
	classNameWithLeastWorkers := lbs.classByDescLoadPct[len(lbs.classByDescLoadPct)-1]
	// fill the jobClasses map with the state of the running jobs
	for requestor, jobs := range jobsByRequestor {
		var jc *jobClass
		var ok bool
		className := GetRequestorClass(requestor, lbs.requestorReToClassMap)
		jc, ok = lbs.jobClasses[className]
		if !ok {
			// the class name was not recognized, use the class with the smallest class load %
			lbs.config.stat.Counter(stats.SchedLBSUnknownJobCounter).Inc(1)
			jc = lbs.jobClasses[classNameWithLeastWorkers]
			log.Errorf("%s is not a recognized job class assigning to class (%s)", className, classNameWithLeastWorkers)
		}
		if jc.origTargetLoadPct == 0 {
			log.Errorf("%s worker allocation (load %% is 0), ignoring %d jobs", requestor, len(jobs))
			lbs.config.stat.Counter(stats.SchedLBSIgnoredJobCounter).Inc(1)
			continue
		}

		// organize the class's jobs by the number of tasks currently running (map of jobs' waiting tasks indexed by the number of
		// tasks currently running for the job).  This will be used in the round robin task selection to start a
		// class's task allocation from jobs with least number of running tasks.
		// The waiting task array for each job preserves the task order from the job defintion, to ensure that jobs' tasks are started
		// in the same order as they are defined in the job definition.
		// This loop also computes the class's running tasks and waiting task totals
		for _, job := range jobs {
			_, ok := jc.jobsByNumRunningTasks[job.TasksRunning]
			if !ok {
				jc.jobsByNumRunningTasks[job.TasksRunning] = []jobWaitingTasks{}
			}
			waitingTasks := []*taskState{}
			for _, taskState := range job.Tasks {
				if taskState.Status == domain.NotStarted {
					waitingTasks = append(waitingTasks, taskState)
					jc.origNumWaitingTasks++
				}
			}
			jc.jobsByNumRunningTasks[job.TasksRunning] = append(jc.jobsByNumRunningTasks[job.TasksRunning], jobWaitingTasks{jobState: job, waitingTasks: waitingTasks})
			if job.TasksRunning > jc.maxTaskRunningMapIndex {
				jc.maxTaskRunningMapIndex = job.TasksRunning
			}
			jc.origNumRunningTasks += job.TasksRunning
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
		lbs.workerLoanAllocation(numIdleWorkers, false)
	}
}

// entitlementTasksToStart compute the number of tasks we can start for each class based on each classes original targeted
// number of workers (origNumTargetedWorkers)
// Note: this is an iterative computation that converges on the number of tasks to start within number of class's iterations.
//
// 1. compute the outstanding entitlement of a class as the class's orig target load minus (number of tasks running + number of tasks
// to start from the prior iteration)  (exception: if a class does not have waiting tasks, its entitlement is 0)
// 2. compute outstanding entitlement % as outstanding entitlement/total of all classes outstanding entitlements
// 3. compute num tasks to start for each class as min(outstanding entitlement % * idle(unallocated) workers, number of the class's waiting tasks)
//
// After completing the 3 steps above, the sum of the number tasks to start may still be < number of idle workers.  This will happen
// when a class's waiting task count < than its outstanding entitlement (the class cannot use all if its entitlement).  When this happens,
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
			break
		}

		// compute normalized entitlement pcts for classes with entitlement > 0
		lbs.computeEntitlementPercents()

		// compute number of tasks to start (workersToAllocate) as per the normalized entitlement %s
		numTasksAllocated := 0
		tasksToStart := min(numIdleWorkers, totalEntitlements)
		numTasksAllocated, haveWaitingTasks = lbs.getTaskAllocations(tasksToStart)

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

// loanWorkers: We have workers that can be 'loaned' to classes that still have waiting tasks.  (That is, a class
// may have more running tasks than their entitlement % because other classes are not using their full entitlement.)
// Note: this is an iterative computation that will converge on the number of workers to loan to classes
// For each iteration
// 1. normalize the original target load % to those classes with waiting tasks
// 2. compute each class's allowed loan amount as the number of unallocated workers * the normalized % but not to
// exceed the class's number of waiting tasks
//
// When a class's allowed loan amount is larger than the class's waiting tasks, there will be unallocated workers
// after all the class 'loan' amounts have been calculated.  When this happens we repeat the loan calculation till
// there are no unallocated workers left.  Each iteration either uses up all idle workers, or all of a class's waiting
// tasks.  This means that the we will not iterate more than the number of classes.
func (lbs *LoadBasedAlg) workerLoanAllocation(numIdleWorkers int, haveRebalanced bool) {
	i := 0
	for ; i < len(lbs.jobClasses); i++ {
		lbs.computeLoanPercents(numIdleWorkers, haveRebalanced)

		// compute loan %'s and allocate idle workers
		numTasksToStart, haveWaitingTasks := lbs.getTaskAllocations(numIdleWorkers)

		numIdleWorkers -= numTasksToStart

		if !haveWaitingTasks {
			break
		}
		if numIdleWorkers <= 0 {
			break
		}
	}
}

// getTaskAllocations - compute the number of tasks to start for each class.
// The class's pct at this point are normalized to factor out class's with no waiting tasks.
// The computed number of tasks will not exceed the class's number of waiting tasks.
//
// This function updates each class' numTasksToStart and returns the total number of tasks to
// start and a boolean indicating if any of the classes still have unallocated tasks
func (lbs *LoadBasedAlg) getTaskAllocations(numIdleWorkers int) (int, bool) {
	totalTasksToStart := 0
	haveWaitingTasks := false

	for _, className := range lbs.classByDescLoadPct {
		jc := lbs.jobClasses[className]
		numTasksToStart := min(jc.numWaitingTasks, ceil(float32(numIdleWorkers)*(float32(jc.tempNormalizedPct)/100.0)))
		if jc.numTasksToStart < 0 {
			// we've determined we need to stop numTasksToStart for this class, but the subsequent loan calculation may
			// have determined this class can also get loaners, we'll reduce the number of tasks to stop by the loaner amount.
			// (below outside this if), but we also want to set the normalization pct to 0 to prevent redoing this reduction
			// if we repeat the loan calculation
			jc.tempNormalizedPct = 0.0
		}

		if (totalTasksToStart + numTasksToStart) > numIdleWorkers {
			numTasksToStart = numIdleWorkers - totalTasksToStart
		}
		jc.numTasksToStart += numTasksToStart
		jc.numWaitingTasks -= numTasksToStart
		if jc.numWaitingTasks > 0 {
			haveWaitingTasks = true
		}
		totalTasksToStart += numTasksToStart
	}
	return totalTasksToStart, haveWaitingTasks
}

// computeEntitlementPercents computes each class's current entitled % of total entitlements (from the current)
// entitlement values.  The entitlement percents are written to the corresponding jobClass.tempNormalizedPCt field.
func (lbs *LoadBasedAlg) computeEntitlementPercents() {
	// get the entitlements total
	entitlementTotal := 0
	for _, jc := range lbs.jobClasses {
		entitlementTotal += jc.tempEntitlement
	}

	// compute the % for all but the class with the largest %.  Add up all computed %s and assign
	// 100 - sum of % to the class with largest % (this eliminates rounding errors, forcing the
	// % to add up to 100%)
	totalPercents := 0
	firstClass := true
	for _, className := range lbs.classByDescLoadPct {
		if firstClass {
			firstClass = false
			continue
		}
		jc := lbs.jobClasses[className]
		jc.tempNormalizedPct = int(math.Floor(float64(jc.tempEntitlement) * 100.0 / float64(entitlementTotal)))
		totalPercents += jc.tempNormalizedPct
	}
	lbs.jobClasses[lbs.classByDescLoadPct[0]].tempNormalizedPct = 100 - totalPercents
}

// computeLoanPercents as orig load %'s normalized to exclude classes that don't have waiting tasks and
// to adjust loan targets based on number of workers already loaned to that class.
// The loan percents percents are written to the corresponding jobClass.tempNormalizedPCt field.
func (lbs *LoadBasedAlg) computeLoanPercents(numWorkersAvailableForLoan int, haveRebalanced bool) {
	// get the sum of all the original load pcts for classes that have waiting tasks
	pctsTotal := 0
	for _, jc := range lbs.jobClasses {
		if jc.numWaitingTasks > 0 {
			pctsTotal += jc.origTargetLoadPct
		}
	}

	if pctsTotal == 0 {
		// there are no workers who can use a loan
		return
	}

	// normalize the original class % excluding classes that don't have waiting tasks.
	// (These pcts will be further adjusted to account for workers already loaned to each class.)
	// Also compute the total number of loaned workers (totalLoaners).
	normalizedLoanPcts := map[string]float64{}
	totalLoaners := 0
	for _, className := range lbs.classByDescLoadPct {
		jc := lbs.jobClasses[className]
		if jc.numWaitingTasks > 0 {
			// normalize the class's loan %
			normalizedLoanPcts[className] = float64(jc.origTargetLoadPct) / float64(pctsTotal)
		} else {
			// exclude the class
			normalizedLoanPcts[className] = 0.0
		}
		if !haveRebalanced {
			// only add up loaners if we are not rebalancing tasks - if we are rebalancing, then the
			// number of loaners are theoretically 0
			totalLoaners += int(math.Max(0.0, float64(jc.origNumRunningTasks-jc.origNumTargetedWorkers)))
		}
	}
	// at this point origTargetLoanPcts is the orig load % normalized to exclude workers who don't have waiting tasks
	// (workers who can't use a loan)

	// update totalLoaners to include the idle workers that are about to be loaned
	totalLoaners += numWorkersAvailableForLoan

	loanEntitlements := map[string]int{}
	totalEntitlements := 0
	// compute the loan entitlement for each class: the class's loan % (origTargetLoanPcts) * totalLoaned workers
	// then subtract the current loaned from the loan entitlement to get the remaining entitlement for each class
	for className, jc := range lbs.jobClasses {
		// compute the number of workers targeted to loan to this class (out of all currently loaned + workers available for loaning)
		entitlement := int(math.Floor(normalizedLoanPcts[className] * float64(totalLoaners)))
		currentLoaned := int(math.Max(0, float64(jc.origNumRunningTasks-jc.origNumTargetedWorkers)))
		if haveRebalanced && jc.numTasksToStart < 0 {
			// if we're going to be stopping tasks, decrease the number currently loaned by the number of tasks we're stopping
			currentLoaned += jc.numTasksToStart
		}

		// reduce that entitlement by the number of workers already loaned
		loanEntitlements[className] = int(math.Max(0, float64(entitlement-currentLoaned)))
		totalEntitlements += loanEntitlements[className]
	}

	// re-normalize the loan entitlement %'s for just those workers whose loan entitlement is still > 0
	// the available workers will be loaned as per these final %'s
	for className, jc := range lbs.jobClasses {
		jc.tempNormalizedPct = int(float64(loanEntitlements[className]) / float64(totalEntitlements) * 100.0)
	}
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
		var jobs []jobWaitingTasks
		var ok bool
		if jobs, ok = jc.jobsByNumRunningTasks[numRunningTasks]; !ok {
			// there are no jobs with numRunningTasks running tasks, move on to jobs with more running tasks
			continue
		}
		// jobs contains list of jobs and their waiting taskIds. (Each job in this list has the same number of running tasks.)
		// Allocate one task from each job till we've allocated numTasksToStart for the jobClass, or have allocated 1 task from
		// each job in this list.  As we allocate a task for a job, move the job to the end of jc.jobsByNumRunningTasks[numRunningTasks+1].
		for _, job := range jobs {
			if job.waitingTasks != nil && len(job.waitingTasks) > 0 {
				// get the next task to start from the job
				tasks = append(tasks, job.waitingTasks[0])

				// move the job to jobsByRunningTasks with numRunningTasks + 1 entry.  Note: we don't have to pull it from
				// its current numRunningTasks bucket since this is a 1 time pass through the jobsByNumRunningTasks map.  The map
				// will be rebuilt with the next scheduling iteration
				if len(job.waitingTasks) > 1 {
					job.waitingTasks = job.waitingTasks[1:]
					jc.jobsByNumRunningTasks[numRunningTasks+1] = append(jc.jobsByNumRunningTasks[numRunningTasks+1], job)
					if numRunningTasks == jc.maxTaskRunningMapIndex {
						jc.maxTaskRunningMapIndex++
					}
				} else {
					job.waitingTasks = []*taskState{}
				}

				startingTaskCnt++
				if startingTaskCnt == jc.numTasksToStart {
					return tasks
				}
			}

		}
	}

	msg := "getTasksToStartForJobClass() fell out of for range jobs loop.  We expected it to return because startingTaskCnt == jc.numTasksToStart above " +
		"before falling out of the loop.  Workers may be left idle till the next scheduling iteration."
	log.Warn(msg)
	return tasks
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
	tasksToStop := []*taskState{}
	for len(tasksToStop) < numTasksToStop {
		key := taskClassAndStartKey{class: jobClass.className, start: startTimeSec}
		tasks := lbs.tasksByJobClassAndStartTimeSec[key]
		for _, task := range tasks {
			tasksToStop = append(tasksToStop, task)
			if len(tasksToStop) == numTasksToStop {
				break
			}
		}
		startTimeSec = startTimeSec.Add(-1 * time.Second).Truncate(time.Second)
		if startTimeSec.Before(earliest) {
			break
		}
	}
	lbs.config.stat.Gauge(fmt.Sprintf("%s%s", stats.SchedStoppingTasks, jobClass.className)).Update(int64(numTasksToStop))
	return tasksToStop
}

func (lbs *LoadBasedAlg) getNumTasksToStart(requestor string) int {
	return lbs.jobClasses[requestor].numTasksToStart
}

// rebalanceClassTasks compute the tasks that should be deleted to allow the scheduling algorithm
// to start tasks in better alignment with the original targeted task load percents.
// The function returns the list of tasks to stop and updates the jobClass objects with the number
// of tasks to start
func (lbs *LoadBasedAlg) rebalanceClassTasks(jobsByRequestor map[string][]*jobState, totalWorkers int) []*taskState {
	log.Info("Rebalancing")
	totalTasks := 0
	// compute number tasks as per the each class's entitlement and waiting tasks
	// will be negative when a class is over its entitlement
	for _, jc := range lbs.jobClasses {
		if jc.origNumRunningTasks > jc.origNumTargetedWorkers {
			// the class is running more than its entitled number of tasks, numTasksToStart is number of tasks
			// to stop to bring back to its entitlement (it will be a negative number)
			jc.numTasksToStart = jc.origNumTargetedWorkers - jc.origNumRunningTasks
		} else if jc.origNumRunningTasks+jc.origNumWaitingTasks < jc.origNumTargetedWorkers {
			// the waiting tasks won't put the class over its entitlement
			jc.numTasksToStart = jc.origNumWaitingTasks
		} else {
			// the number of tasks that could be started to bring the class up to its entitlement
			jc.numTasksToStart = jc.origNumTargetedWorkers - jc.origNumRunningTasks
		}
		totalTasks += jc.origNumRunningTasks + jc.numTasksToStart
	}

	if totalTasks < totalWorkers {
		// some classes are not using their full allocation, we can loan workers
		lbs.computeLoanPercents(totalWorkers-totalTasks, true)

		lbs.getTaskAllocations(totalWorkers - totalTasks)
	}

	tasksToStop := lbs.buildTaskStopList()

	return tasksToStop
}

// getCurrentPercentsSpread is used to measure how well the current running tasks counts match the target loads.
// It computes each class's difference between the target load pct and actual load pct.  The 'spread' value
// is the difference between the min and max differences across the classes.  Eg: if 30% was the load target
// for class A and class A is using 50% of the workers, A's pct difference is -20%.  If class B has a target
// of 15% and is only using 5% of workers, B's pct difference is 10%.  If A and B are the only classes, the
// pctSpread is 25% (5% - -20%).
func (lbs *LoadBasedAlg) getCurrentPercentsSpread(totalWorkers int) int {
	if len(lbs.jobClasses) < 2 {
		return 0
	}
	minPct := 0
	maxPct := 0
	for _, className := range lbs.classByDescLoadPct {
		jc := lbs.jobClasses[className]
		currPct := int(math.Floor(float64(jc.origNumRunningTasks) * 100.0 / float64(totalWorkers)))
		pctDiff := jc.origTargetLoadPct - currPct
		if pctDiff < 0 || jc.numWaitingTasks > 0 { // only consider classes using loaned workers, or with waiting tasks
			minPct = min(minPct, pctDiff)
			maxPct = max(maxPct, pctDiff)
		}
	}

	return maxPct - minPct
}

// getClassByDescLoadPct get a copy of the config's class by descending load pcts
func (lbs *LoadBasedAlg) getClassByDescLoadPct() []string {
	lbs.config.classLoadPercentsMu.RLock()
	defer lbs.config.classLoadPercentsMu.RUnlock()
	copy := []string{}
	for _, v := range lbs.config.classByDescLoadPct {
		copy = append(copy, v)
	}
	return copy
}

// getClassLoadPercents return a copy of the ClassLoadPercents converting to int32
func (lbs *LoadBasedAlg) getClassLoadPercents() map[string]int32 {
	lbs.config.classLoadPercentsMu.RLock()
	defer lbs.config.classLoadPercentsMu.RUnlock()
	copy := map[string]int32{}
	for k, v := range lbs.config.classLoadPercents {
		copy[k] = int32(v)
	}
	return copy
}

// LocalCopyClassLoadPercents return a copy of the ClassLoadPercents leaving as int
func (lbs *LoadBasedAlg) LocalCopyClassLoadPercents() map[string]int {
	lbs.config.classLoadPercentsMu.RLock()
	defer lbs.config.classLoadPercentsMu.RUnlock()
	copy := map[string]int{}
	for k, v := range lbs.config.classLoadPercents {
		copy[k] = v
	}
	return copy
}

// setClassLoadPercents set the scheduler's class load pcts with a copy of the input class load pcts
func (lbs *LoadBasedAlg) setClassLoadPercents(classLoadPercents map[string]int32) {
	lbs.config.classLoadPercentsMu.Lock()
	defer lbs.config.classLoadPercentsMu.Unlock()

	// build a list that orders the classes by descending pct.
	keys := []string{}
	for key := range classLoadPercents {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return classLoadPercents[keys[i]] > classLoadPercents[keys[j]]
	})
	lbs.config.classByDescLoadPct = keys

	// set the load pcts - normalizing them if the don't add up to 100
	lbs.config.classLoadPercents = map[string]int{}
	pctTotal := 0
	for k, val := range classLoadPercents {
		lbs.config.classLoadPercents[k] = int(val)
		pctTotal += int(val)
	}
	if pctTotal != 100 {
		log.Errorf("LoadBalanced scheduling %%'s don't add up to 100, normalizing them")
		lbs.normalizePercents(pctTotal)
	}

	log.Infof("classLoadPercents are %v", lbs.config.classLoadPercents)
}

// normalizePercents normalizes the class percents to the sum of their values.  Only uses when the configured
// percents don't add up to 100
func (lbs *LoadBasedAlg) normalizePercents(percentsSum int) {
	totalNormalizedPercents := 0
	firstClass := true
	normalizedPercents := map[string]int{}
	for _, className := range lbs.config.classByDescLoadPct {
		if firstClass {
			firstClass = false
			continue // skip the first class (highest %), it will be given the difference between 100 and the sum of the other %'s
		}
		classPercent := lbs.config.classLoadPercents[className]
		normalizedPercents[className] = int(math.Floor(float64(classPercent) / float64(percentsSum) * 100))
		totalNormalizedPercents += normalizedPercents[className]
	}
	normalizedPercents[lbs.config.classByDescLoadPct[0]] = 100 - totalNormalizedPercents
	lbs.config.classLoadPercents = normalizedPercents
}

// getRequestorToClassMap return a copy of the RequestorToClassMap
func (lbs *LoadBasedAlg) getRequestorToClassMap() map[string]string {
	lbs.config.requestorReToClassMapMU.RLock()
	defer lbs.config.requestorReToClassMapMU.RUnlock()
	copy := map[string]string{}
	for k, v := range lbs.config.requestorReToClassMap {
		copy[k] = v
	}
	return copy
}

// setRequestorToClassMap set the scheduler's requestor to class map with a copy of the input map
func (lbs *LoadBasedAlg) setRequestorToClassMap(requestorToClassMap map[string]string) {
	lbs.config.requestorReToClassMapMU.Lock()
	defer lbs.config.requestorReToClassMapMU.Unlock()
	lbs.config.requestorReToClassMap = map[string]string{}
	for k, v := range requestorToClassMap {
		lbs.config.requestorReToClassMap[k] = v
	}
	log.Infof("set requestorToClassMap to %v", requestorToClassMap)
}

// getRebalanceMinimumDuration get the rebalance duration
func (lbs *LoadBasedAlg) getRebalanceMinimumDuration() time.Duration {
	lbs.config.rebalanceMinDurationMu.RLock()
	defer lbs.config.rebalanceMinDurationMu.RUnlock()
	return lbs.config.rebalanceMinDuration
}

// setRebalanceMinimumDuration set the rebalance duration
func (lbs *LoadBasedAlg) setRebalanceMinimumDuration(rebalanceMinDuration time.Duration) {
	lbs.config.rebalanceMinDurationMu.Lock()
	defer lbs.config.rebalanceMinDurationMu.Unlock()
	lbs.config.rebalanceMinDuration = rebalanceMinDuration
	log.Infof("set rebalanceMinDuration to %s", rebalanceMinDuration)
}

// getRebalanceThreshold get the rebalance threshold
func (lbs *LoadBasedAlg) getRebalanceThreshold() int {
	lbs.config.rebalanceThresholdMu.RLock()
	defer lbs.config.rebalanceThresholdMu.RUnlock()
	return lbs.config.rebalanceThreshold
}

// setRebalanceThreshold set the rebalance thresold
func (lbs *LoadBasedAlg) setRebalanceThreshold(rebalanceThreshold int) {
	lbs.config.rebalanceThresholdMu.Lock()
	defer lbs.config.rebalanceThresholdMu.Unlock()
	lbs.config.rebalanceThreshold = rebalanceThreshold
	log.Infof("set rebalanceThreshold to %d", rebalanceThreshold)
}

func (lbs *LoadBasedAlg) GetDataStructureSizeStats() map[string]int {
	return map[string]int{
		stats.SchedLBSConfigDescLoadPctSize:      len(lbs.config.classByDescLoadPct),
		stats.SchedLBSConfigLoadPercentsSize:     len(lbs.config.classLoadPercents),
		stats.SchedLBSConfigRequestorToPctsSize:  len(lbs.config.requestorReToClassMap),
		stats.SchedLBSWorkingDescLoadPctSize:     len(lbs.classByDescLoadPct),
		stats.SchedLBSWorkingJobClassesSize:      lbs.getJobClassesSize(),
		stats.SchedLBSWorkingLoadPercentsSize:    len(lbs.classByDescLoadPct),
		stats.SchedLBSWorkingRequestorToPctsSize: len(lbs.requestorReToClassMap),
	}
}

func (lbs *LoadBasedAlg) getJobClassesSize() int {
	// get sum of number of waiting tasks across all the jobClasses's job entries
	s := 0
	for _, jc := range lbs.jobClasses {
		for _, wt := range jc.jobsByNumRunningTasks {
			s += len(wt)
		}
	}
	return s
}
