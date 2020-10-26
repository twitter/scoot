package server

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/scheduler/domain"
)

var (
	default_sc = SchedulerConfig{
		MaxRetriesPerTask:       0,
		DebugMode:               false,
		RecoverJobsOnStartup:    false,
		DefaultTaskTimeout:      0,
		TaskTimeoutOverhead:     0,
		RunnerRetryTimeout:      0,
		RunnerRetryInterval:     0,
		ReadyFnBackoff:          0,
		MaxRequestors:           0,
		MaxJobsPerRequestor:     0,
		SoftMaxSchedulableTasks: 0,
		TaskThrottle:            0,
		Admins:                  nil,
	}
)

type classState struct {
	loadPct              int32
	numRunningTasks      int
	numWaitingTasks      int
	numJobs              int
	expectedTasksToStart int
}

type testDef struct {
	totalWorkers int
	classes      map[string]classState
}

func Test_Class_Task_Start_Cnts(t *testing.T) {
	testsDefs := []testDef{
		{totalWorkers: 1000, classes: map[string]classState{
			"c0": classState{loadPct: 30, numRunningTasks: 200, numWaitingTasks: 290, numJobs: 10, expectedTasksToStart: 94},
			"c1": classState{loadPct: 25, numRunningTasks: 300, numWaitingTasks: 230, numJobs: 50, expectedTasksToStart: 0},
			"c2": classState{loadPct: 20, numRunningTasks: 0, numWaitingTasks: 150, numJobs: 20, expectedTasksToStart: 150},
			"c3": classState{loadPct: 15, numRunningTasks: 100, numWaitingTasks: 150, numJobs: 3, expectedTasksToStart: 46},
			"c4": classState{loadPct: 10, numRunningTasks: 110, numWaitingTasks: 90, numJobs: 2, expectedTasksToStart: 0},
			"c5": classState{loadPct: 0, numRunningTasks: 0, numWaitingTasks: 328, numJobs: 1, expectedTasksToStart: 0},
		}},
		{totalWorkers: 1000, classes: map[string]classState{
			"c0": classState{loadPct: 30, numRunningTasks: 200, numWaitingTasks: 290, numJobs: 10, expectedTasksToStart: 167},
			"c1": classState{loadPct: 25, numRunningTasks: 300, numWaitingTasks: 230, numJobs: 15, expectedTasksToStart: 53},
			"c2": classState{loadPct: 20, numRunningTasks: 0, numWaitingTasks: 0, numJobs: 0, expectedTasksToStart: 0},
			"c3": classState{loadPct: 15, numRunningTasks: 100, numWaitingTasks: 50, numJobs: 3, expectedTasksToStart: 50},
			"c4": classState{loadPct: 10, numRunningTasks: 110, numWaitingTasks: 90, numJobs: 2, expectedTasksToStart: 20},
		}},
		{totalWorkers: 1000, classes: map[string]classState{
			"c0": classState{loadPct: 30, numRunningTasks: 200, numWaitingTasks: 10, numJobs: 2, expectedTasksToStart: 10},
			"c1": classState{loadPct: 25, numRunningTasks: 300, numWaitingTasks: 230, numJobs: 15, expectedTasksToStart: 166},
			"c2": classState{loadPct: 20, numRunningTasks: 0, numWaitingTasks: 0, numJobs: 0, expectedTasksToStart: 0},
			"c3": classState{loadPct: 15, numRunningTasks: 100, numWaitingTasks: 50, numJobs: 10, expectedTasksToStart: 50},
			"c4": classState{loadPct: 10, numRunningTasks: 110, numWaitingTasks: 90, numJobs: 3, expectedTasksToStart: 64},
		}},
		{totalWorkers: 1000, classes: map[string]classState{
			"c0": classState{loadPct: 30, numRunningTasks: 0, numWaitingTasks: 300, numJobs: 30, expectedTasksToStart: 105},
			"c1": classState{loadPct: 25, numRunningTasks: 0, numWaitingTasks: 230, numJobs: 10, expectedTasksToStart: 81},
			"c2": classState{loadPct: 20, numRunningTasks: 0, numWaitingTasks: 400, numJobs: 40, expectedTasksToStart: 66},
			"c3": classState{loadPct: 15, numRunningTasks: 0, numWaitingTasks: 650, numJobs: 13, expectedTasksToStart: 48},
			"c4": classState{loadPct: 10, numRunningTasks: 700, numWaitingTasks: 800, numJobs: 40, expectedTasksToStart: 0},
		}},
		{totalWorkers: 1000, classes: map[string]classState{
			"c0": classState{loadPct: 35, numRunningTasks: 200, numWaitingTasks: 100, numJobs: 30, expectedTasksToStart: 100},
			"c1": classState{loadPct: 30, numRunningTasks: 300, numWaitingTasks: 50, numJobs: 10, expectedTasksToStart: 0},
			"c2": classState{loadPct: 20, numRunningTasks: 0, numWaitingTasks: 200, numJobs: 40, expectedTasksToStart: 159},
			"c3": classState{loadPct: 0, numRunningTasks: 100, numWaitingTasks: 300, numJobs: 13, expectedTasksToStart: 0},
			"c4": classState{loadPct: 15, numRunningTasks: 110, numWaitingTasks: 500, numJobs: 40, expectedTasksToStart: 31},
		}},
		{totalWorkers: 10000, classes: map[string]classState{
			"c0": classState{loadPct: 30, numRunningTasks: 1660, numWaitingTasks: 14220, numJobs: 300, expectedTasksToStart: 830},
			"c1": classState{loadPct: 25, numRunningTasks: 101, numWaitingTasks: 9401, numJobs: 100, expectedTasksToStart: 1282},
			"c2": classState{loadPct: 16, numRunningTasks: 420, numWaitingTasks: 16542, numJobs: 400, expectedTasksToStart: 641},
			"c3": classState{loadPct: 14, numRunningTasks: 14, numWaitingTasks: 4194, numJobs: 13, expectedTasksToStart: 754},
			"c4": classState{loadPct: 6, numRunningTasks: 404, numWaitingTasks: 15944, numJobs: 400, expectedTasksToStart: 76},
			"c5": classState{loadPct: 4, numRunningTasks: 42, numWaitingTasks: 11136, numJobs: 40, expectedTasksToStart: 187},
			"c6": classState{loadPct: 3, numRunningTasks: 977, numWaitingTasks: 9145, numJobs: 30, expectedTasksToStart: 0},
			"c7": classState{loadPct: 2, numRunningTasks: 2612, numWaitingTasks: 16781, numJobs: 40, expectedTasksToStart: 0},
		}},
	}

	statsRegistry := stats.NewFinagleStatsRegistry()
	statsReceiver, _ := stats.NewCustomStatsReceiver(func() stats.StatsRegistry { return statsRegistry }, 0)

	jobsByJobID := map[string]*jobState{}
	for _, testDef := range testsDefs {
		totalWorkers := testDef.totalWorkers
		usedWorkers := 0
		jobsByRequestor := map[string][]*jobState{}
		requestorToClass := map[string]string{}
		loadPcts := map[string]int32{}
		expectedNumTasks := 0
		for className, state := range testDef.classes {
			usedWorkers += state.numRunningTasks
			requestor := fmt.Sprintf("requestor%s", className)
			jobsByRequestor[requestor] = makeJobStateFromClassState(t, requestor, state, jobsByJobID)
			requestorToClass[requestor] = className
			loadPcts[className] = state.loadPct
			expectedNumTasks += state.expectedTasksToStart
		}

		cluster := &clusterState{
			updateCh:         nil,
			nodes:            nil,
			suspendedNodes:   nil,
			offlinedNodes:    nil,
			nodeGroups:       makeIdleGroup(totalWorkers),
			maxLostDuration:  0,
			maxFlakyDuration: 0,
			readyFn:          nil,
			numRunning:       usedWorkers,
			stats:            nil,
		}
		cluster.nodes = cluster.nodeGroups["idle"].idle

		lbs := NewLoadBasedAlg(loadPcts, requestorToClass, statsReceiver)

		tasksToBeAssigned := lbs.GetTasksToBeAssigned(nil, statsReceiver, cluster, jobsByRequestor, default_sc)

		assert.Equal(t, expectedNumTasks, len(tasksToBeAssigned), "wrong number of tasks in tasksToBeAssigned")

		// compute the number of tasks to start for each class from the tasks list
		numTasksByClassName := map[string]int{}
		for _, task := range tasksToBeAssigned {
			jobState := jobsByJobID[task.JobId]
			className := lbs.getRequestorClass(jobState.Job.Def.Requestor)
			if _, ok := numTasksByClassName[className]; !ok {
				numTasksByClassName[className] = 1
			} else {
				numTasksByClassName[className]++
			}
		}

		// verify we've computed the number of tasks to start for each task correctly and
		// have the correct number of tasks in the task list for each class
		for className, state := range testDef.classes {
			// verify the computed number of tasks to start for the class
			assert.Equal(t, state.expectedTasksToStart, lbs.getNumTasksToStart(className), "wrong number of tasks to start for class %s", className)
			assert.Equal(t, state.expectedTasksToStart, numTasksByClassName[className], "wrong number of %s tasks in the task list", className)
		}
	}
}

// TestRandomScenario generate random tests with 10k workers and verify that
// the idle workers are allocated
func TestRandomScenario(t *testing.T) {
	// set up the test scenario: set up 2 classes to get 75% of workers, then create random % for the remaining 25%
	loadPcts := generatePcts()

	aTest := testDef{totalWorkers: 10000, classes: map[string]classState{}}
	totalWorkers := aTest.totalWorkers
	// define a random set of class states for the loadPcts defined above
	// these classes will use up a random number of workers (not to exceed 5000) and the number of waiting
	// tasks for each class will be a random number, not to exceed 2 times the total number of workers
	workersToUse := totalWorkers - rand.Intn(5001)
	totalWaitingTasks := 0
	for className := range loadPcts {
		numRunningTasks := 0
		if workersToUse > 0 {
			numRunningTasks = rand.Intn(workersToUse + 1)
		}
		waitingTasks := rand.Intn(totalWorkers * 2)
		aTest.classes[className] = classState{numRunningTasks: numRunningTasks, numWaitingTasks: waitingTasks, numJobs: int(math.Min(100.0, float64(numRunningTasks)))}
		workersToUse -= numRunningTasks
		totalWaitingTasks += waitingTasks
	}

	// create jobState objects for each class
	usedWorkers := 0
	jobsByRequestor := map[string][]*jobState{}
	jobsByJobID := map[string]*jobState{}
	requestorToClass := map[string]string{}
	for className, state := range aTest.classes {
		usedWorkers += state.numRunningTasks
		requestor := fmt.Sprintf("requestor%s", className)
		jobsByRequestor[requestor] = makeJobStateFromClassState(t, requestor, state, jobsByJobID)
		requestorToClass[requestor] = className
	}

	cluster := &clusterState{
		updateCh:         nil,
		nodes:            nil,
		suspendedNodes:   nil,
		offlinedNodes:    nil,
		nodeGroups:       makeIdleGroup(totalWorkers),
		maxLostDuration:  0,
		maxFlakyDuration: 0,
		readyFn:          nil,
		numRunning:       usedWorkers,
		stats:            nil,
	}
	cluster.nodes = cluster.nodeGroups["idle"].idle

	statsRegistry := stats.NewFinagleStatsRegistry()
	statsReceiver, _ := stats.NewCustomStatsReceiver(func() stats.StatsRegistry { return statsRegistry }, 0)

	// run the test
	lbs := NewLoadBasedAlg(loadPcts, requestorToClass, statsReceiver)

	tasks := lbs.GetTasksToBeAssigned(nil, statsReceiver, cluster, jobsByRequestor, default_sc)

	// verify the results: we don't know what to expect for each class (since it was randomly generated), so
	// just verify that the number of tasks being started equals the min of number of idle workers, total waiting tasks
	numTasksStarting := 0
	for className := range aTest.classes {
		numTasksStarting += lbs.getNumTasksToStart(className)
	}

	expectedNumTasks := int(math.Min(float64(totalWorkers-usedWorkers), float64(totalWaitingTasks)))
	assert.Equal(t, totalWorkers-usedWorkers, numTasksStarting)
	assert.Equal(t, expectedNumTasks, len(tasks))
	// assert.True(t, false)
}

func generatePcts() map[string]int32 {
	// set up the test scenario: set up 2 classes to get 75% of workers, then create random % for the remaining 25%
	rand.Seed(time.Now().UnixNano())
	loadPcts := map[string]int32{
		"c0": 50,
		"c1": 25,
	}
	// generate random %s to make up the remaining 25 %
	i := 2
	var remainingPct int32 = 25
	for remainingPct > 0 {
		var pct int32
		if remainingPct < 3 {
			pct = remainingPct
		} else {
			pct = int32(rand.Intn(10)) // pct will be 0-9
		}
		loadPcts[fmt.Sprintf("c%d", i)] = pct
		i++
		remainingPct -= pct
	}

	return loadPcts
}

// makeJobStateFromClassState make a list of jobStates for the class.  The classState will contain the number of
// jobStates to create and the total number of running and waiting tasks to distribute across the jobStates.
func makeJobStateFromClassState(t *testing.T, className string, cState classState, jobsByJobID map[string]*jobState) []*jobState {
	jobStates := make([]*jobState, cState.numJobs)

	var runningTasksDist []int
	if cState.numRunningTasks > 0 && cState.numJobs > 0 {
		runningTasksDist = createTaskDistribution(cState.numRunningTasks, cState.numJobs)
	}
	var waitingTasksDist []int
	if cState.numWaitingTasks != 0 && cState.numJobs > 0 {
		waitingTasksDist = createTaskDistribution(cState.numWaitingTasks, cState.numJobs)
	}

	totalWaitingTasks := 0
	totalRunningTasks := 0
	for i := 0; i < cState.numJobs; i++ {
		var numRunningTasks int = 0
		if cState.numRunningTasks != 0 {
			numRunningTasks = runningTasksDist[i]
		}
		var numWaitingTasks int = 0
		if cState.numWaitingTasks != 0 {
			numWaitingTasks = waitingTasksDist[i]
		}
		j := &domain.Job{
			Id: fmt.Sprintf("job_%s_%d", className, i),
			Def: domain.JobDefinition{
				JobType:   "dummyJobType",
				Requestor: className,
				Basis:     "",
				Tag:       "",
				Priority:  domain.Priority(0),
			},
		}
		js := &jobState{
			Job:            j,
			Saga:           nil,
			EndingSaga:     false,
			TasksCompleted: 0,
			TasksRunning:   numRunningTasks,
			JobKilled:      false,
			TimeCreated:    time.Time{},
			TimeMarker:     time.Time{},
			Completed:      make(map[string]*taskState),
			Running:        nil,
			NotStarted:     nil,
		}
		totalRunningTasks += numRunningTasks

		t, ts := makeDummyTasks(j.Id, numWaitingTasks)
		j.Def.Tasks = t
		js.Tasks = ts
		taskMap := makeTaskMap(ts)
		js.NotStarted = taskMap
		jobStates[i] = js
		jobsByJobID[js.Job.Id] = js

		totalWaitingTasks += len(taskMap)
	}

	if (cState.numWaitingTasks != totalWaitingTasks) || (cState.numRunningTasks != totalRunningTasks) {
		// this is an error print the configuration so we can debug it
		log.Debugf("****** cState:%v", cState)
		assert.Equal(t, cState.numWaitingTasks, totalWaitingTasks, "invalid test setup, did not create correct number of waiting tasks for %s", className)
		assert.Equal(t, cState.numRunningTasks, totalRunningTasks, "invalid test setup, did not create correct number of running tasks for %s", className)
	}

	return jobStates
}

// createTaskDistribution a distribution of n tasks to m jobs.  Create list of m integers such that the sum
// of the integers add up to n.
func createTaskDistribution(nTasks int, mJobs int) []int {
	rand.Seed(time.Now().UnixNano())

	// over m iterations generate random numbers making sure the sum doesn't go over nTasks
	totalTaskCnt := 0
	taskCnts := []int{}
	if nTasks < mJobs {
		for i := 0; i < mJobs; i++ {
			if totalTaskCnt < nTasks {
				taskCnts = append(taskCnts, 1)
				totalTaskCnt++
			} else {
				taskCnts = append(taskCnts, 0)
			}
		}
	} else {
		aveTasksPerJob := int(nTasks/mJobs) * 2
		// generate random numbers up to aveTasksPerJob, forcing the final entries to add up to the sum
		for i := 0; i < mJobs; i++ {
			t := rand.Intn(aveTasksPerJob) + 1
			if (nTasks - (totalTaskCnt + t)) <= (mJobs - i) {
				taskCnts = append(taskCnts, 1)
				totalTaskCnt++
			} else {
				taskCnts = append(taskCnts, t)
				totalTaskCnt += t
			}
		}
		if totalTaskCnt < nTasks {
			taskCnts[len(taskCnts)-1] += nTasks - totalTaskCnt
		}
	}
	return taskCnts
}
