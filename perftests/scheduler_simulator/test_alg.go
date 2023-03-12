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

are a map of job definitions where the map index is the number of seconds (in the overall simulation) to delay before starting
this job

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
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/wisechengyi/scoot/cloud/cluster"
	cc "github.com/wisechengyi/scoot/cloud/cluster"
	"github.com/wisechengyi/scoot/common"
	"github.com/wisechengyi/scoot/common/stats"
	"github.com/wisechengyi/scoot/runner"
	"github.com/wisechengyi/scoot/saga"
	"github.com/wisechengyi/scoot/saga/sagalogs"
	"github.com/wisechengyi/scoot/scheduler/domain"
	"github.com/wisechengyi/scoot/scheduler/server"
)

type externalDeps struct {
	// external components used by scheduler
	nodesUpdatesCh  chan []cc.NodeUpdate
	sc              saga.SagaCoordinator
	fakeRunners     func(cc.Node) runner.Service
	nodeToWorkerMap map[string]runner.Service
	statsRegistry   stats.StatsRegistry
	statsReceiver   stats.StatsReceiver
	latchTime       time.Duration
	statsCancelFn   func()
}

type fakeFetcher struct {
	nodes []cc.Node
}

func (ff *fakeFetcher) setNodeList(nodes []cc.Node) {
	ff.nodes = nodes
}
func (ff *fakeFetcher) Fetch() ([]cc.Node, error) {
	return ff.nodes, nil
}

type timeSummary struct {
	// structure for storing summary info about the job
	buildUrl     string        // the original build url
	prodDuration time.Duration // the production duration from the log
	testStart    time.Time     // when the build was started in the test
	testEnd      time.Time
}

type SchedulingAlgTester struct {
	extDeps             *externalDeps
	statsFileName       string
	testsStart          time.Time
	testsEnd            time.Time
	realStart           time.Time
	firstJobStartOffset time.Duration
	jobDefsMap          map[int][]*domain.JobDefinition
	pRatios             []int
	clusterSize         int
	finishTimeFilename  string
	comparisonMap       map[string]*timeSummary
	comparisonMapMu     sync.RWMutex
	timeout             time.Duration
	classLoadPercents   map[string]int32
	requestorToClassMap map[string]string
	sortedClassNames    []string
}

/*
Make a SchedulingAlgTester object

jobDefsMap is a map to the job definitions (each task in the job definition
contains the number of seconds the task should take during the simulation)
the map keys are the number of seconds (from the start of the simulation) to delay
before starting the job
*/
func MakeSchedulingAlgTester(testsStart, testsEnd time.Time, jobDefsMap map[int][]*domain.JobDefinition,
	clusterSize int, classLoadPercents map[string]int32, requestorToClassMap map[string]string) *SchedulingAlgTester {

	tDir := fmt.Sprintf("%sCloudExec", os.TempDir())
	if _, err := os.Stat(tDir); os.IsNotExist(err) {
		os.Mkdir(tDir, 0777)
	}

	dateTimeZone := "2006_01_02_15_04_05_MST"
	startTimeStr := testsStart.Format(dateTimeZone)
	endTimeStr := testsEnd.Format(dateTimeZone)
	statsFile := fmt.Sprintf("%s/newAlgStats%s_%s.csv", tDir, startTimeStr, endTimeStr)
	finishTimesFilename := fmt.Sprintf("%s/newAlgJobTimes%s_%s.csv", tDir, startTimeStr, endTimeStr)

	numJobs := 0
	for _, jds := range jobDefsMap {
		numJobs += len(jds)
	}

	log.Warn(".........................")
	log.Warnf("Stats are being written to %s", statsFile)
	log.Warnf("Final comparisons are being written to %s", finishTimesFilename)
	log.Warnf("Test will shadow %s to %s", testsStart.Format(time.RFC3339), testsEnd.Format(time.RFC3339))
	log.Warnf("Running %d jobs", numJobs)
	log.Warnf("On %d workers", clusterSize)
	log.Warnf("Using Class Loads %v", classLoadPercents)
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
		classLoadPercents:   classLoadPercents,
		requestorToClassMap: requestorToClassMap,
	}

	keys := []string{}
	for key := range classLoadPercents {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return strings.Compare(keys[i], keys[j]) < 0
	})
	st.sortedClassNames = keys

	st.makeComparisonMap()
	st.writeFirstLines()
	return st
}

func (st *SchedulingAlgTester) RunTest() error {
	st.extDeps = st.getExternals(st.clusterSize)

	config := st.getTestConfig()
	s := server.NewStatefulScheduler(
		st.extDeps.nodesUpdatesCh,
		st.extDeps.sc,
		st.extDeps.fakeRunners,
		config,
		st.extDeps.statsReceiver,
		nil,
		nil,
	)
	s.SetClassLoadPercents(st.classLoadPercents)
	s.SetRequestorToClassMap(st.requestorToClassMap)

	sc := s.GetSagaCoord()

	// start a go routine printing the stats
	stopStatsCh := make(chan bool)
	go st.printStats(stopStatsCh)

	// set up goroutine picking up job completion times
	allJobsDoneCh := make(chan bool)    // true when all jobs have finished
	allJobsStartedCh := make(chan bool) // used this channel to tell the watchForAllDone that it has all job ids
	realStart := time.Now()
	realEnd := realStart.Add(st.testsEnd.Sub(st.testsStart))
	go st.watchForAllDone(allJobsStartedCh, allJobsDoneCh, realEnd, sc)

	// initialize structures for running the jobs
	// sort the job map so we run them in ascending time order
	secondsAfterStartKeys := make([]int, 0)
	for k := range st.jobDefsMap {
		secondsAfterStartKeys = append(secondsAfterStartKeys, k)
	}
	sort.Ints(secondsAfterStartKeys)

	// now start running the jobs at the same frequency that they were run in production
	log.Warnf("%s: Starting the sim for jobs running after %s.", realStart.Format(time.RFC3339), st.testsStart.Format(time.RFC3339))
	if len(st.jobDefsMap) == 0 {
		log.Errorf("no jobs")
		return nil
	}
	st.firstJobStartOffset = time.Duration(secondsAfterStartKeys[0]) * time.Second
	for _, secondsAfterStart := range secondsAfterStartKeys {
		jobDefs := st.jobDefsMap[secondsAfterStart]
		for _, jobDef := range jobDefs {
			select {
			case <-allJobsDoneCh:
				// if an error occurred in watchForAllDone, abort the test
				return fmt.Errorf("error reported looking for completed jobs.  See log")
			default:
			}
			// startAt is when the simulation should start the jobs with respect to the current (wall) clock time
			startAt := realStart.Add(time.Duration(secondsAfterStart) * time.Second).Add(-1 * st.firstJobStartOffset)
			if time.Now().Before(startAt) {
				pause := startAt.Sub(time.Now())
				log.Warnf("waiting %s to start the next job", pause.Truncate(time.Second))
				<-time.After(pause) // wait till time reaches startTime
			}

			// give the job to the scheduler
			simTime := st.testsStart.Add(st.firstJobStartOffset).Add(time.Now().Sub(realStart))
			log.Warnf("%s(%s): submitting job:%s", simTime.Format(time.RFC3339), time.Duration(secondsAfterStart)*time.Second-st.firstJobStartOffset, jobDef)
			id, err := s.ScheduleJob(*jobDef)
			if err != nil {
				return fmt.Errorf("Expected job to be Scheduled Successfully %v", err)
			}
			if id == "" {
				return fmt.Errorf("Expected successfully scheduled job to return non empty job string!")
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
			log.Errorf("didn't get and end time for %s", timeSummary.buildUrl)
		}
	}
	return nil
}

/*
watch for jobs completing.  Record finish times. When all the jobs
have finished put the finished times on an all done channel
*/
func (st *SchedulingAlgTester) watchForAllDone(allJobsStartedCh chan bool,
	allJobsDoneCh chan bool, endTime time.Time, sc saga.SagaCoordinator) {
	finishedJobs := make(map[string]bool) // if the job is in this map, it has finished
	finalCnt := -1
	for true {
		jobIds := st.getComparisonMapKeys()
		select {
		case <-allJobsStartedCh:
			finalCnt = len(jobIds)
		default:
		}
		// look for newly completed jobs, record their finish times
		allDone := false
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
				}
			}
			if finalCnt > 0 && len(finishedJobs) == finalCnt {
				allDone = true
				break
			}
		}
		if allDone {
			allJobsDoneCh <- true
			return
		}
		if !time.Now().Before(endTime) {
			// the sim duration is up, discard all jobs still running, their timings will not be accurate since
			// the simulation is not adding any more real load to the workers
			for _, id := range jobIds {
				if _, ok := finishedJobs[id]; !ok {
					timeSummary := st.getComparisonMapEntry(id)
					log.Warnf("timing out job %s", timeSummary.buildUrl)
					finishedJobs[id] = true
					st.recordJobEndTime(id, true)
				}
			}
			allJobsDoneCh <- true
			return
		}
		time.Sleep(3 * time.Second)
	}
}

/*
store the production duration and the test start time for a job id in the ComparisonMapEntry
*/
func (st *SchedulingAlgTester) makeTimeSummary(jobDef *domain.JobDefinition, jobId string) error {
	re := regexp.MustCompile("url:(.*), elapsedMin:([0-9]+)")
	m := re.FindStringSubmatch(jobDef.Tag)
	buildUrl := m[1]
	prodDurationStr := m[2]
	prodDuration, e := strconv.Atoi(prodDurationStr)
	if e != nil {
		return fmt.Errorf("couldn't parse elapsedMin value:%s, %s", prodDurationStr, e.Error())
	}
	ts := &timeSummary{
		buildUrl:     buildUrl,
		prodDuration: time.Duration(prodDuration) * time.Minute,
		testStart:    time.Now(),
		testEnd:      time.Unix(0, 0),
	}

	st.setComparisonMapEntry(ts, jobId)

	return nil
}

func (st *SchedulingAlgTester) getExternals(clusterSize int) *externalDeps {
	nodes := []string{}
	for i := 0; i < clusterSize; i++ {
		nodes = append(nodes, fmt.Sprintf("node%d", i))
	}
	nodeUpdateCh := make(chan []cc.NodeUpdate, common.DefaultClusterChanSize)
	initTestCluster(nodeUpdateCh, nodes...)

	statsReg := stats.NewFinagleStatsRegistry()
	latchTime := time.Minute
	st.timeout = 2 * time.Hour
	statsRec, cancelFn := stats.NewCustomStatsReceiver(func() stats.StatsRegistry { return statsReg }, latchTime)

	return &externalDeps{
		nodesUpdatesCh: nodeUpdateCh,
		sc:             sagalogs.MakeInMemorySagaCoordinatorNoGC(nil),
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
	simTime := st.testsStart.Add(elapsed).Add(-1 * st.firstJobStartOffset)
	timePP := simTime.Format(time.RFC3339)
	statsJson := st.extDeps.statsReceiver.Render(false)
	var s map[string]interface{}
	json.Unmarshal(statsJson, &s)
	line := []byte(fmt.Sprintf("%s", timePP))
	for _, className := range st.sortedClassNames {
		runningStatName := fmt.Sprintf("schedNumRunningTasksGauge_%s", className)
		waitingStatName := fmt.Sprintf("schedNumWaitingTasksGauge_%s", className)
		runningCnt, _ := s[runningStatName]
		waitingCnt, _ := s[waitingStatName]
		line = append(line, []byte(fmt.Sprintf("class:%s, running:%v, waiting:%v, ",
			className, runningCnt, waitingCnt))...)
	}
	line = append(line, '\n')

	f, _ := os.OpenFile(st.statsFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	defer f.Close()
	f.Write(line)
	log.Warnf("%s\n", line)
}

func (st *SchedulingAlgTester) recordJobEndTime(jobId string, timedOut bool) error {
	finish := time.Now()
	timeSummary := st.getComparisonMapEntry(jobId)
	timeSummary.testEnd = finish
	testTime := finish.Sub(timeSummary.testStart)
	delta := timeSummary.prodDuration - testTime
	var line string
	if timedOut {
		line = fmt.Sprintf("%s, delta,%d, prod,%d, (seconds), test, %d, (seconds), timedOut\n",
			timeSummary.buildUrl, int(delta.Seconds()), int(timeSummary.prodDuration.Seconds()), int(testTime.Seconds()))
	} else {
		line = fmt.Sprintf("%s, delta,%d, prod,%d, (seconds), test, %d, (seconds)\n",
			timeSummary.buildUrl, int(delta.Seconds()), int(timeSummary.prodDuration.Seconds()), int(testTime.Seconds()))
	}

	f, _ := os.OpenFile(st.finishTimeFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	defer f.Close()
	f.Write([]byte(line))

	return nil
}

func (st *SchedulingAlgTester) getTestConfig() server.SchedulerConfiguration {
	return server.SchedulerConfiguration{
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

func (st *SchedulingAlgTester) setComparisonMapEntry(ts *timeSummary, jobId string) {
	st.comparisonMapMu.Lock()
	defer st.comparisonMapMu.Unlock()
	st.comparisonMap[jobId] = ts
}

func (st *SchedulingAlgTester) writeFirstLines() {
	f, _ := os.OpenFile(st.finishTimeFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	f1, _ := os.OpenFile(st.statsFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	defer f.Close()
	defer f1.Close()
	line := fmt.Sprintf("runDate: %s, testWindow: %s, %s, class loads: %v, requestor map:%v\n",
		time.Now().Format("2006-01-02 15:04 MST"), st.testsStart.Format("2006-01-02 15:04 MST"),
		st.testsEnd.Format("2006-01-02 15:04 MST"), st.classLoadPercents, st.requestorToClassMap)
	f.Write([]byte(line))
	f1.Write([]byte(line))
}

func initNodeUpdateChan(nodes ...string) chan []cc.NodeUpdate {
	uc := make(chan []cc.NodeUpdate, common.DefaultClusterChanSize)
	updates := []cc.NodeUpdate{}
	for _, node := range nodes {
		updates = append(updates, cc.NewAdd(cc.NewIdNode(node)))
	}
	uc <- updates
	return uc
}

func initTestCluster(nodesUpdateCh chan []cc.NodeUpdate, nodes ...string) {
	nodeUpdates := []cc.NodeUpdate{}
	for _, n := range nodes {
		nodeUpdates = append(nodeUpdates, cc.NewAdd(cc.NewIdNode(n)))
	}
	nodesUpdateCh <- nodeUpdates
}
