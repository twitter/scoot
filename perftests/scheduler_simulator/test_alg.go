/*
Scheduling algorithm evaluation framework.
The objective is to determine if a scheduling algorithm provides better throughput than
the current production scheduling algorithm.  The framework runs on a standalone machine
running the scheduler with the new algorithm and emulating the production delays for each
task.  Its output is a comparison of the shadow job duration to the original production
job duration and intermediate stats on tasks running and waiting.

Input parameters (provided to the SchedulingAlgTester constructor):
 - test start time: the actual time the jobs being shadowed started
 - test end time: the actual time the jobs being shadowed ended
 - job definitions: a set of 'shadow' job definitions where each job 'shadows' a real job.  The 'shadow' job definitions
are a map of job definitions where the map index can be used to sort the jobs to relative run order.
   . the job_definition.Basis the number of nanoseconds to wait before starting this job. (We use this to ensure the
simulation load mirrors the jobs it is shadowing.)
   . each task's Command.Argv[1] entry is the number of milliseconds the worker should wait before returning success/failure
   . each task's Command.Argv[2] entry is the task's exit code
*/
package scheduler_simulator

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/saga/sagalogs"
	"github.com/twitter/scoot/scheduler/domain"
	"github.com/twitter/scoot/scheduler/server"
)

type externalDeps struct {
	// external components used by scheduler
	initialCl       []cluster.Node
	clUpdates       chan []cluster.NodeUpdate
	sc              saga.SagaCoordinator
	fakeRunners     func(cluster.Node) runner.Service
	nodeToWorkerMap map[string]runner.Service
	statsRegistry   stats.StatsRegistry
	statsReceiver   stats.StatsReceiver
	latchTime       time.Duration
	statsCancelFn   func()
}

/*
fake cluster
*/
type testCluster struct {
	ch    chan []cluster.NodeUpdate
	nodes []cluster.Node
}

type timeSummary struct {
	// structure for storing summary info about the job
	buildURL     string        // the original build url
	prodDuration time.Duration // the production duration from the log
	testStart    time.Time     // when the build was started in the test
	testEnd      time.Time
}

// SchedulingAlgTester runs the scheduling algorithm collecting load/throughput summaries
type SchedulingAlgTester struct {
	extDeps             *externalDeps
	statsFileName       string
	testsStart          time.Time
	testsEnd            time.Time
	realStart           time.Time
	jobDefsMap          map[int][]*domain.JobDefinition
	pRatios             []int
	clusterSize         int
	finishTimeFilename  string
	comparisonMap       map[string]*timeSummary
	comparisonMapMu     sync.RWMutex
	timeout             time.Duration
	classLoadPcts       map[string]int32
	requestorToClassMap map[string]string
}

/*
MakeSchedulingAlgTester Make a SchedulingAlgTester object

jobDefsMap is a map of relative start time (seconds) -> a job definition where each task in the job definition
contains the number of seconds the task should take during the simulation

*/
func MakeSchedulingAlgTester(testsStart, testsEnd time.Time, jobDefsMap map[int][]*domain.JobDefinition,
	clusterSize int, classLoadPcts map[string]int32, requestorToClassMap map[string]string) *SchedulingAlgTester {

	tDir := fmt.Sprintf("%sCloudExec", os.TempDir())
	if _, err := os.Stat(tDir); os.IsNotExist(err) {
		os.Mkdir(tDir, 0777)
	}

	statsFile := fmt.Sprintf("%s/newAlgStats.csv", tDir)
	finishTimesFilename := fmt.Sprintf("%s/newAlgJobTimes.csv", tDir)

	log.Warn(".........................")
	log.Warnf("Stats are being written to %s", statsFile)
	log.Warnf("Final comparisons are being written to %s", finishTimesFilename)
	log.Warnf("Test will shadow %s to %s", testsStart.Format(time.RFC3339), testsEnd.Format(time.RFC3339))
	log.Warnf("Running %d jobs", len(jobDefsMap))
	log.Warnf("On %d workers", clusterSize)
	log.Warnf("Using Class Loads %v", classLoadPcts)
	log.Warnf("Using Requestor Map %v", requestorToClassMap)
	log.Warn(".........................")
	st := &SchedulingAlgTester{
		statsFileName:       statsFile,
		finishTimeFilename:  finishTimesFilename,
		realStart:           time.Now(),
		testsStart:          testsStart,
		testsEnd:            testsEnd,
		jobDefsMap:          jobDefsMap,
		clusterSize:         clusterSize,
		classLoadPcts:       classLoadPcts,
		requestorToClassMap: requestorToClassMap,
	}
	st.makeComparisonMap()
	st.writeFirstLines()
	return st
}

// RunTest start running the test
func (st *SchedulingAlgTester) RunTest() error {
	st.extDeps = st.getExternals(st.clusterSize)

	config := st.getTestConfig()
	s := server.NewStatefulScheduler(
		st.extDeps.initialCl,
		st.extDeps.clUpdates,
		st.extDeps.sc,
		st.extDeps.fakeRunners,
		config,
		st.extDeps.statsReceiver,
	)
	s.SetClassLoadPcts(st.classLoadPcts)
	s.SetRequestorToClassMap(st.requestorToClassMap)

	sc := s.GetSagaCoord()

	// start a go routine printing the stats
	stopStatsCh := make(chan bool)
	go st.printStats(stopStatsCh)

	// set up goroutine picking up job completion times
	allJobsDoneCh := make(chan bool)    // true when all jobs have finished
	allJobsStartedCh := make(chan bool) // used this channel to tell the watchForAllDone that it has all job ids
	go st.watchForAllDone(allJobsStartedCh, allJobsDoneCh, sc)

	// initialize structures for running the jobs
	shadowStart := time.Now()
	// sort the job map so we run them in ascending time order
	keys := make([]int, 0)
	for k := range st.jobDefsMap {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	// now start running the jobs at the same frequency that they were run in production
	log.Warnf("%s: Starting %d jobs.", shadowStart.Format(time.RFC3339), len(st.jobDefsMap))
	if len(st.jobDefsMap) == 0 {
		log.Errorf("no jobs")
		return nil
	}
	for _, key := range keys {
		jobDefs := st.jobDefsMap[key]
		for secondsAfterStart, jobDef := range jobDefs {
			select {
			case <-allJobsDoneCh:
				// if an error occurred in watchForAllDone, abort the test
				return fmt.Errorf("error reported looking for completed jobs.  See log")
			default:
			}
			// // pause to simulate the frequency in which the jobs arrived in production
			// deltaFromStart, e := st.extractWaitDurationFromJobDef(jobDef)
			// if e != nil {
			// 	return fmt.Errorf("Couldn't get deltaStartDuration:%s, skipping job", e.Error())
			// }
			n := time.Now()
			startTime := shadowStart.Add(time.Duration(secondsAfterStart) * time.Second)
			if startTime.After(n) {
				sleepDuration := startTime.Sub(n)
				time.Sleep(sleepDuration) // this pause emulates the jobs' run frequency
			}

			// give the job to the scheduler
			id, err := s.ScheduleJob(*jobDef)
			if err != nil {
				return fmt.Errorf("Expected job to be Scheduled Successfully %v", err)
			}
			if id == "" {
				return fmt.Errorf("Expected successfully scheduled job to return non empty job string")
			}

			err = st.makeTimeSummary(jobDef, id) // record job start time, and production elapsed time
			if err != nil {
				return fmt.Errorf("error extracting prod time from %s. %s", jobDef.Tag, err.Error())
			}
		}
	}
	allJobsStartedCh <- true // tell watchForAllDone that it will not get any more job ids

	log.Warn(".........................")
	log.Warn("all jobs have been started, waiting for final tasks to complete\n")
	log.Warn(".........................")
	<-allJobsDoneCh // wait for go routine collecting job times to report them back

	// shut down stats
	stopStatsCh <- true // stop the timed stats collection/printing

	st.writeStatsToFile()      // write the final stats
	st.extDeps.statsCancelFn() // stop stats collectors

	for _, timeSummary := range st.comparisonMap {
		if timeSummary.testEnd == time.Unix(0, 0) {
			log.Errorf("didn't get and end time for %s", timeSummary.buildURL)
		}
	}
	return nil
}

/*
watch for jobs completing.  Record finish times. When all the jobs
have finished put the finished times on an all done channel
*/
func (st *SchedulingAlgTester) watchForAllDone(allJobsStartedCh chan bool,
	allJobsDoneCh chan bool, sc saga.SagaCoordinator) {
	finishedJobs := make(map[string]bool) // if the job is in this map, it has finished
	allDone := false
	finalCnt := -1
	for !allDone {
		jobIds := st.getComparisonMapKeys()
		select {
		case <-allJobsStartedCh:
			finalCnt = len(jobIds)
		default:
		}
		// look for newly completed jobs, record their finish times
		for _, id := range jobIds {
			if _, ok := finishedJobs[id]; !ok {
				// we haven't seen the job finish yet, check its state
				s, _ := sc.GetSagaState(id)
				if s.IsSagaCompleted() || s.IsSagaAborted() {
					// the job is newly finished, record its time
					finishedJobs[id] = true
					err := st.recordJobEndTime(id, false)
					if err != nil {
						log.Errorf("error writing comparison time for %s: %s", id, err.Error())
						allDone = true
						break
					}

				} else {
					// timeout the unfinished job?
					timeSummary := st.getComparisonMapEntry(id)
					if time.Now().Sub(timeSummary.testStart) > st.timeout {
						log.Warnf("timing out job %s", timeSummary.buildURL)
						finishedJobs[id] = true
						st.recordJobEndTime(id, true)
					}
				}
			}
			if finalCnt > 0 && len(finishedJobs) == finalCnt {
				allDone = true
				break
			}
		}
		if !allDone {
			time.Sleep(3 * time.Second)
		}
	}

	allJobsDoneCh <- true
}

/*
store the production duration and the test start time for a job id in the ComparisonMapEntry
*/
func (st *SchedulingAlgTester) makeTimeSummary(jobDef *domain.JobDefinition, jobID string) error {
	re := regexp.MustCompile("url:(.*), elapsedMin:([0-9]+)")
	m := re.FindStringSubmatch(jobDef.Tag)
	buildURL := m[1]
	prodDurationStr := m[2]
	prodDuration, e := strconv.Atoi(prodDurationStr)
	if e != nil {
		return fmt.Errorf("couldn't parse elapsedMin value:%s, %s", prodDurationStr, e.Error())
	}
	ts := &timeSummary{
		buildURL:     buildURL,
		prodDuration: time.Duration(prodDuration) * time.Minute,
		testStart:    time.Now(),
		testEnd:      time.Unix(0, 0),
	}

	st.setComparisonMapEntry(ts, jobID)

	return nil
}

// // extract the time from Basis field
// func (st *SchedulingAlgTester) extractWaitDurationFromJobDef(jobDef *domain.JobDefinition) (time.Duration, error) {
// 	d, e := strconv.Atoi(jobDef.Basis)
// 	if e != nil {
// 		return time.Duration(0), fmt.Errorf("couldn't parse duration from job def basis:%s", e.Error())
// 	}
// 	return time.Duration(d), nil
// }

func (st *SchedulingAlgTester) getExternals(clusterSize int) *externalDeps {

	cl := st.makeTestCluster(clusterSize)
	statsReg := stats.NewFinagleStatsRegistry()
	latchTime := time.Minute
	st.timeout = 2 * time.Hour
	statsRec, cancelFn := stats.NewCustomStatsReceiver(func() stats.StatsRegistry { return statsReg }, latchTime)

	return &externalDeps{
		initialCl: cl.nodes,
		clUpdates: cl.ch,
		sc:        sagalogs.MakeInMemorySagaCoordinatorNoGC(),
		fakeRunners: func(n cluster.Node) runner.Service {
			return makeFakeWorker(n)
		},
		statsRegistry: stats.NewFinagleStatsRegistry(),
		statsReceiver: statsRec,
		statsCancelFn: cancelFn,
		latchTime:     latchTime,
	}
}

// use in a goroutine to print stats every minute
func (st *SchedulingAlgTester) printStats(stopCh chan bool) {
	ticker := time.NewTicker(st.extDeps.latchTime)

	for true {
		select {
		case <-ticker.C:
			st.writeStatsToFile()
		case <-stopCh:
			return
		}
	}
}

func (st *SchedulingAlgTester) writeStatsToFile() {
	t := time.Now()
	elapsed := t.Sub(st.realStart)
	simTime := st.testsStart.Add(elapsed)
	timePP := simTime.Format(time.RFC3339)
	statsJSON := st.extDeps.statsReceiver.Render(false)
	var s map[string]interface{}
	json.Unmarshal(statsJSON, &s)
	line := make([]byte, 0)
	for className, loadPct := range st.classLoadPcts {
		runningStatName := fmt.Sprintf("schedNumRunningTasksGauge_%s", className)
		waitingStatName := fmt.Sprintf("schedNumWaitingTasksGauge_%s", className)
		runningCnt, ok1 := s[runningStatName]
		waitingCnt, ok2 := s[waitingStatName]
		if ok1 || ok2 {
			line = append(line, []byte(fmt.Sprintf("%s, classLoad:(%s, %d), running:%v, waiting:%v, ",
				timePP, className, loadPct, runningCnt, waitingCnt))...)
		}
	}
	line = append(line, '\n')

	f, _ := os.OpenFile(st.statsFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	defer f.Close()
	f.Write(line)
	log.Warnf("%s\n", line)
}

func (st *SchedulingAlgTester) recordJobEndTime(jobID string, timedOut bool) error {
	finish := time.Now()
	timeSummary := st.getComparisonMapEntry(jobID)
	timeSummary.testEnd = finish
	testTime := finish.Sub(timeSummary.testStart)
	delta := timeSummary.prodDuration - testTime
	var line string
	if timedOut {
		line = fmt.Sprintf("%s, delta,%d, prod,%d, (seconds), test, %d, (seconds), timedOut\n",
			timeSummary.buildURL, int(delta.Seconds()), int(timeSummary.prodDuration.Seconds()), int(testTime.Seconds()))
	} else {
		line = fmt.Sprintf("%s, delta,%d, prod,%d, (seconds), test, %d, (seconds)\n",
			timeSummary.buildURL, int(delta.Seconds()), int(timeSummary.prodDuration.Seconds()), int(testTime.Seconds()))
	}

	f, _ := os.OpenFile(st.finishTimeFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	defer f.Close()
	f.Write([]byte(line))

	return nil
}

func (st *SchedulingAlgTester) makeTestCluster(num int) *testCluster {
	h := &testCluster{
		ch: make(chan []cluster.NodeUpdate, 1),
	}
	nodes := []cluster.Node{}
	for i := 0; i < num; i++ {
		nodes = append(nodes, cluster.NewIdNode(fmt.Sprintf("node%d", i)))
	}
	h.nodes = nodes
	return h
}

func (st *SchedulingAlgTester) getTestConfig() server.SchedulerConfig {
	return server.SchedulerConfig{
		MaxRetriesPerTask:    0,
		DebugMode:            false,
		RecoverJobsOnStartup: false,
		DefaultTaskTimeout:   0,
		TaskTimeoutOverhead:  0,
		RunnerRetryTimeout:   0,
		RunnerRetryInterval:  0,
		ReadyFnBackoff:       0,
		MaxRequestors:        1000,
		MaxJobsPerRequestor:  1000,
		TaskThrottle:         0,
		Admins:               nil,
	}
}

func (st *SchedulingAlgTester) getRequestorMap(jobDefsMap map[int][]*domain.JobDefinition) map[domain.Priority]string {
	m := make(map[domain.Priority]string)

	var r string
	for _, jobDefs := range jobDefsMap {
		for _, jobDef := range jobDefs {
			r = jobDef.Requestor
			m[jobDef.Priority] = r
		}
	}

	return m
}

func (st *SchedulingAlgTester) getComparisonMapEntry(id string) *timeSummary {
	st.comparisonMapMu.RLock()
	defer st.comparisonMapMu.RUnlock()
	return st.comparisonMap[id]
}

func (st *SchedulingAlgTester) makeComparisonMap() {
	st.comparisonMapMu.Lock()
	defer st.comparisonMapMu.Unlock()
	st.comparisonMap = make(map[string]*timeSummary)
}

func (st *SchedulingAlgTester) getComparisonMapKeys() []string {
	st.comparisonMapMu.RLock()
	defer st.comparisonMapMu.RUnlock()
	keys := make([]string, len(st.comparisonMap))
	i := 0
	for k := range st.comparisonMap {
		keys[i] = k
		i++
	}
	return keys
}

func (st *SchedulingAlgTester) setComparisonMapEntry(ts *timeSummary, jobID string) {
	st.comparisonMapMu.Lock()
	defer st.comparisonMapMu.Unlock()
	st.comparisonMap[jobID] = ts
}

func (st *SchedulingAlgTester) writeFirstLines() {
	f, _ := os.OpenFile(st.finishTimeFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	f1, _ := os.OpenFile(st.statsFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	defer f.Close()
	defer f1.Close()
	line := fmt.Sprintf("runDate: %s, testWindow: %s, %s, class loads: %v, requestor map:%v\n",
		time.Now().Format("2006-01-02 15:04 MST"), st.testsStart.Format("2006-01-02 15:04 MST"),
		st.testsEnd.Format("2006-01-02 15:04 MST"), st.classLoadPcts, st.requestorToClassMap)
	f.Write([]byte(line))
	f1.Write([]byte(line))
}
