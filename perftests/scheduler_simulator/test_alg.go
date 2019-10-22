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
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
*/
=======
 */
>>>>>>> scheduler algorithm and eval framework re-org
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
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
	"github.com/twitter/scoot/scheduler/domain"
	"github.com/twitter/scoot/scheduler/server"
=======
	"github.com/twitter/scoot/sched"
	"github.com/twitter/scoot/sched/scheduler"
>>>>>>> scheduler algorithm and eval framework re-org
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
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
	buildUrl     string        // the original build url
	prodDuration time.Duration // the production duration from the log
	testStart    time.Time     // when the build was started in the test
	testEnd      time.Time
}

type SchedulingAlgTester struct {
	extDeps            *externalDeps
	statsFileName      string
	testsStart         time.Time
	testsEnd           time.Time
	realStart          time.Time
	jobDefsMap         map[int][]*domain.JobDefinition
	pRatios            []int
	clusterSize        int
	finishTimeFilename string
	comparisonMap      map[string]*timeSummary
	comparisonMapMu    sync.RWMutex
	timeout            time.Duration
=======
	buildUrl     string	       // the original build url
	prodDuration time.Duration // the production duration from the log
	testStart time.Time        // when the build was started in the test
	testEnd	  time.Time
}

type SchedulingAlgTester struct {
	extDeps *externalDeps
	statsFileName string
	testsStart time.Time
	testsEnd   time.Time
	realStart time.Time
	jobDefsMap map[int][]*sched.JobDefinition
	pRatios []int
	clusterSize int
	finishTimeFilename string
	comparisonMap map[string]*timeSummary
	comparisonMapMu sync.RWMutex
	timeout time.Duration
>>>>>>> scheduler algorithm and eval framework re-org
}

/*
Make a SchedulingAlgTester object

jobDefsMap is a map of relative start time (seconds) -> a job definition where each task in the job definition
contains the number of seconds the task should take during the simulation

<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
*/
func MakeSchedulingAlgTester(testsStart, testsEnd time.Time, jobDefsMap map[int][]*domain.JobDefinition,
	pRatios []int, clusterSize int) *SchedulingAlgTester {

=======
 */
func MakeSchedulingAlgTester(testsStart, testsEnd time.Time, jobDefsMap map[int][]*sched.JobDefinition,
	pRatios []int, clusterSize int) *SchedulingAlgTester {
>>>>>>> scheduler algorithm and eval framework re-org
	tDir := fmt.Sprintf("%sCloudExec", os.TempDir())
	if _, err := os.Stat(tDir); os.IsNotExist(err) {
		os.Mkdir(tDir, 0777)
	}

	statsFile := fmt.Sprintf("%s/newAlgStats.csv", tDir)
	finishTimesFilename := fmt.Sprintf("%s/newAlgJobTimes.csv", tDir)

<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
=======

>>>>>>> scheduler algorithm and eval framework re-org
	log.Warn(".........................")
	log.Warnf("Stats are being written to %s", statsFile)
	log.Warnf("Final comparisons are being written to %s", finishTimesFilename)
	log.Warnf("Test will shadow %s to %s", testsStart.Format(time.RFC3339), testsEnd.Format(time.RFC3339))
	log.Warnf("Running %d jobs", len(jobDefsMap))
	log.Warnf("On %d workers", clusterSize)
	log.Warnf("Using Ratios %v", pRatios)
	log.Warn(".........................")
	st := &SchedulingAlgTester{
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
		statsFileName:      statsFile,
		finishTimeFilename: finishTimesFilename,
		realStart:          time.Now(),
		testsStart:         testsStart,
		testsEnd:           testsEnd,
		jobDefsMap:         jobDefsMap,
		pRatios:            pRatios,
		clusterSize:        clusterSize,
=======
		statsFileName: statsFile,
		finishTimeFilename: finishTimesFilename,
		realStart: time.Now(),
		testsStart: testsStart,
		testsEnd: testsEnd,
		jobDefsMap: jobDefsMap,
		pRatios: pRatios,
		clusterSize: clusterSize,
>>>>>>> scheduler algorithm and eval framework re-org
	}
	st.makeComparisonMap()
	st.writeFirstLines()
	return st
}

func (st *SchedulingAlgTester) RunTest() error {
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
	if err := st.verifyRatios(); err != nil {
		return err
	}

	st.extDeps = st.getExternals(st.clusterSize)

	config := st.getTestConfig()
	s := server.NewStatefulScheduler(
=======
	st.extDeps = st.getExternals(st.clusterSize)

	config := st.getTestConfig()
	s := scheduler.NewStatefulScheduler(
>>>>>>> scheduler algorithm and eval framework re-org
		st.extDeps.initialCl,
		st.extDeps.clUpdates,
		st.extDeps.sc,
		st.extDeps.fakeRunners,
		config,
		st.extDeps.statsReceiver,
	)

<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
	s.SchedAlg = server.MakePriorityBasedAlg(st.pRatios[:]) // use the priority based algorithm
=======
	s.SchedAlg = scheduler.MakePriorityBasedAlg(st.pRatios[:])  // use the priority based algorithm
>>>>>>> scheduler algorithm and eval framework re-org

	rm := st.getRequestorMap(st.jobDefsMap)

	sc := s.GetSagaCoord()

	// start a go routine printing the stats
	stopStatsCh := make(chan bool)
	go st.printStats(st.extDeps, stopStatsCh, rm)

	// set up goroutine picking up job completion times
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
	allJobsDoneCh := make(chan bool)    // true when all jobs have finished
	allJobsStartedCh := make(chan bool) // used this channel to tell the watchForAllDone that it has all job ids
=======
	allJobsDoneCh := make(chan bool)          // true when all jobs have finished
	allJobsStartedCh := make(chan bool)                 // used this channel to tell the watchForAllDone that it has all job ids
>>>>>>> scheduler algorithm and eval framework re-org
	go st.watchForAllDone(allJobsStartedCh, allJobsDoneCh, sc)

	// initialize structures for running the jobs
	shadowStart := time.Now()
	// sort the job map so we run them in ascending time order
	keys := make([]int, 0)
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
	for k := range st.jobDefsMap {
=======
	for k, _ := range st.jobDefsMap {
>>>>>>> scheduler algorithm and eval framework re-org
		keys = append(keys, k)
	}
	sort.Ints(keys)

	// now start running the jobs at the same frequency that they were run in production
	log.Warnf("%s: Starting %d jobs.", shadowStart.Format(time.RFC3339), len(st.jobDefsMap))
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
	if len(st.jobDefsMap) == 0 {
=======
	if len(st.jobDefsMap) == 0  {
>>>>>>> scheduler algorithm and eval framework re-org
		log.Errorf("no jobs")
		return nil
	}
	for _, key := range keys {
		jobDefs := st.jobDefsMap[key]
		for _, jobDef := range jobDefs {
			select {
			case <-allJobsDoneCh:
				// if an error occurred in watchForAllDone, abort the test
				return fmt.Errorf("error reported looking for completed jobs.  See log.")
			default:
			}
			// pause to simulate the frequency in which the jobs arrived in production
			deltaFromStart, e := st.extractWaitDurationFromJobDef(jobDef)
			if e != nil {
				return fmt.Errorf("Couldn't get deltaStartDuration:%s, skipping job", e.Error())
			}
			n := time.Now()
			startTime := shadowStart.Add(deltaFromStart)
			if startTime.After(n) {
				time.Sleep(startTime.Sub(n)) // this pause emulates the jobs' run frequency
			}

			// give the job to the scheduler
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
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
	allJobsStartedCh <- true // tell watchForAllDone that it will not get any more job ids
=======
	allJobsStartedCh <- true  // tell watchForAllDone that it will not get any more job ids
>>>>>>> scheduler algorithm and eval framework re-org

	log.Warn(".........................")
	log.Warn("all jobs have been started, waiting for final tasks to complete\n")
	log.Warn(".........................")
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
	<-allJobsDoneCh // wait for go routine collecting job times to report them back

	// shut down stats
	stopStatsCh <- true // stop the timed stats collection/printing

	st.writeStatsToFile(st.extDeps, rm) // write the final stats
	st.extDeps.statsCancelFn()          // stop stats collectors
=======
	 <-allJobsDoneCh // wait for go routine collecting job times to report them back

	// shut down stats
	stopStatsCh <- true    // stop the timed stats collection/printing

	st.writeStatsToFile(st.extDeps, rm) // write the final stats
	st.extDeps.statsCancelFn()  // stop stats collectors
>>>>>>> scheduler algorithm and eval framework re-org

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
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
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
=======
 */
func (st *SchedulingAlgTester) watchForAllDone(allJobsStartedCh chan bool,
	allJobsDoneCh chan bool, sc saga.SagaCoordinator) {
	finishedJobs := make(map[string] bool)  // if the job is in this map, it has finished
	allDone := false
	finalCnt := -1
	for ! allDone  {
		jobIds := st.getComparisonMapKeys()
		select {
		case <- allJobsStartedCh:
>>>>>>> scheduler algorithm and eval framework re-org
			finalCnt = len(jobIds)
		default:
		}
		// look for newly completed jobs, record their finish times
		for _, id := range jobIds {
			if _, ok := finishedJobs[id]; !ok {
				// we haven't seen the job finish yet, check its state
				s, _ := sc.GetSagaState(id)
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
				if s.IsSagaCompleted() || s.IsSagaAborted() {
=======
				if s.IsSagaCompleted() || s.IsSagaAborted(){
>>>>>>> scheduler algorithm and eval framework re-org
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
						log.Warnf("timing out job %s", timeSummary.buildUrl)
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
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
*/
func (st *SchedulingAlgTester) makeTimeSummary(jobDef *domain.JobDefinition, jobId string) error {
=======
 */
func (st *SchedulingAlgTester) makeTimeSummary(jobDef *sched.JobDefinition, jobId string) error {
>>>>>>> scheduler algorithm and eval framework re-org
	re := regexp.MustCompile("url:(.*), elapsedMin:([0-9]+)")
	m := re.FindStringSubmatch(jobDef.Tag)
	buildUrl := m[1]
	prodDurationStr := m[2]
	prodDuration, e := strconv.Atoi(prodDurationStr)
	if e != nil {
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
		return fmt.Errorf("couldn't parse elapsedMin value:%s, %s", prodDurationStr, e.Error())
	}
	ts := &timeSummary{
		buildUrl:     buildUrl,
		prodDuration: time.Duration(prodDuration) * time.Minute,
		testStart:    time.Now(),
		testEnd:      time.Unix(0, 0),
=======
		return fmt.Errorf("couldn't parse elapsedMin value:%s, %s", prodDurationStr,e.Error() )
	}
	ts := &timeSummary{
		buildUrl: buildUrl,
		prodDuration: time.Duration(prodDuration) * time.Minute,
		testStart:		time.Now(),
		testEnd: time.Unix(0, 0),
>>>>>>> scheduler algorithm and eval framework re-org
	}

	st.setComparisonMapEntry(ts, jobId)

	return nil
}

/*
extract the time from Basis field
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
*/
func (st *SchedulingAlgTester) extractWaitDurationFromJobDef(jobDef *domain.JobDefinition) (time.Duration, error) {
=======
 */
func (st *SchedulingAlgTester) extractWaitDurationFromJobDef(jobDef *sched.JobDefinition) (time.Duration, error) {
>>>>>>> scheduler algorithm and eval framework re-org
	d, e := strconv.Atoi(jobDef.Basis)
	if e != nil {
		return time.Duration(0), fmt.Errorf("couldn't parse duration from job def basis:%s", e.Error())
	}
	return time.Duration(d), nil
}

<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
func (st *SchedulingAlgTester) getExternals(clusterSize int) *externalDeps {
=======
func (st *SchedulingAlgTester) getExternals (clusterSize int) *externalDeps {
>>>>>>> scheduler algorithm and eval framework re-org

	cl := st.makeTestCluster(clusterSize)
	statsReg := stats.NewFinagleStatsRegistry()
	latchTime := time.Minute
	st.timeout = 2 * time.Hour
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
	statsRec, cancelFn := stats.NewCustomStatsReceiver(func() stats.StatsRegistry { return statsReg }, latchTime)
=======
	statsRec, cancelFn:= stats.NewCustomStatsReceiver(func() stats.StatsRegistry { return statsReg }, latchTime)

>>>>>>> scheduler algorithm and eval framework re-org

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
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
		latchTime:     latchTime,
=======
		latchTime: latchTime,
>>>>>>> scheduler algorithm and eval framework re-org
	}
}

// use in a goroutine to print stats every minute
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
func (st *SchedulingAlgTester) printStats(deps *externalDeps, stopCh chan bool, rm map[domain.Priority]string) {
=======
func (st *SchedulingAlgTester) printStats(deps *externalDeps, stopCh chan bool, rm map[sched.Priority] string) {
>>>>>>> scheduler algorithm and eval framework re-org
	ticker := time.NewTicker(deps.latchTime)

	for true {
		select {
		case <-ticker.C:
			st.writeStatsToFile(deps, rm)
		case <-stopCh:
			return
		}
	}
}

<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
func (st *SchedulingAlgTester) writeStatsToFile(deps *externalDeps, rm map[domain.Priority]string) {
=======
func (st *SchedulingAlgTester) writeStatsToFile(deps *externalDeps, rm map[sched.Priority] string) {
>>>>>>> scheduler algorithm and eval framework re-org
	t := time.Now()
	elapsed := t.Sub(st.realStart)
	simTime := st.testsStart.Add(elapsed)
	timePP := simTime.Format(time.RFC3339)
	statsJson := deps.statsReceiver.Render(false)
	var s map[string]interface{}
	json.Unmarshal(statsJson, &s)
	line := make([]byte, 0)
	for priority := 0; priority < len(rm); priority++ {
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
		req := rm[domain.Priority(priority)]
=======
		req := rm[sched.Priority(priority)]
>>>>>>> scheduler algorithm and eval framework re-org
		runningStatName := fmt.Sprintf("schedNumRunningTasksGauge_%s", req)
		waitingStatName := fmt.Sprintf("schedNumWaitingTasksGauge_%s", req)
		runningCnt := s[runningStatName]
		waitingCnt := s[waitingStatName]
		line = append(line, []byte(fmt.Sprintf("%s, job:%s, priority:%d, running:%v, waiting:%v, ",
			timePP, req, priority, runningCnt, waitingCnt))...)
	}
	line = append(line, '\n')

<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
	f, _ := os.OpenFile(st.statsFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	defer f.Close()
	f.Write(line)
	log.Warnf("%s\n", line)
=======
	f,_ := os.OpenFile(st.statsFileName, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0777)
	defer f.Close()
	f.Write(line)
	log.Warnf("%s\n",line)
>>>>>>> scheduler algorithm and eval framework re-org
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

<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
	f, _ := os.OpenFile(st.finishTimeFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
=======
	f,_ := os.OpenFile(st.finishTimeFilename, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0777)
>>>>>>> scheduler algorithm and eval framework re-org
	defer f.Close()
	f.Write([]byte(line))

	return nil
}

func (st *SchedulingAlgTester) makeTestCluster(num int) *testCluster {
	h := &testCluster{
		ch: make(chan []cluster.NodeUpdate, 1),
	}
	nodes := []cluster.Node{}
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
	for i := 0; i < num; i++ {
=======
	for i := 0; i< num; i++ {
>>>>>>> scheduler algorithm and eval framework re-org
		nodes = append(nodes, cluster.NewIdNode(fmt.Sprintf("node%d", i)))
	}
	h.nodes = nodes
	return h
}

<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
func (st *SchedulingAlgTester) getTestConfig() server.SchedulerConfig {
	return server.SchedulerConfig{
=======
func (st *SchedulingAlgTester) getTestConfig() scheduler.SchedulerConfig {
	return scheduler.SchedulerConfig{
>>>>>>> scheduler algorithm and eval framework re-org
		MaxRetriesPerTask:       0,
		DebugMode:               false,
		RecoverJobsOnStartup:    false,
		DefaultTaskTimeout:      0,
		TaskTimeoutOverhead:     0,
		RunnerRetryTimeout:      0,
		RunnerRetryInterval:     0,
		ReadyFnBackoff:          0,
		MaxRequestors:           1000,
		MaxJobsPerRequestor:     1000,
		SoftMaxSchedulableTasks: 0,
		TaskThrottle:            0,
		Admins:                  nil,
	}
}

<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
func (st *SchedulingAlgTester) getRequestorMap(jobDefsMap map[int][]*domain.JobDefinition) map[domain.Priority]string {
	m := make(map[domain.Priority]string)
=======
func (st *SchedulingAlgTester) getRequestorMap(jobDefsMap map[int][]*sched.JobDefinition) map[sched.Priority] string {
	m := make(map[sched.Priority] string)
>>>>>>> scheduler algorithm and eval framework re-org

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
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
	for k := range st.comparisonMap {
=======
	for k, _ := range st.comparisonMap {
>>>>>>> scheduler algorithm and eval framework re-org
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
	line := fmt.Sprintf("runDate: %s, testWindow: %s, %s, ratios: %v\n",
		time.Now().Format("2006-01-02 15:04 MST"), st.testsStart.Format("2006-01-02 15:04 MST"),
		st.testsEnd.Format("2006-01-02 15:04 MST"), st.pRatios)
	f.Write([]byte(line))
	f1.Write([]byte(line))
<<<<<<< 9d092d08cd3690547626da4daf5cf4ef5590e4af
}

func (st *SchedulingAlgTester) verifyRatios() error {
	// verify the pRatios values start with 1, are ascending and no duplicates
	vals := make(map[int]int)
	for i, ratio := range st.pRatios {
		if i == 0 && ratio != 1 {
			return fmt.Errorf("first ratio must be 1 not %d", ratio)
		} else {
			if i > 0 && st.pRatios[i] < st.pRatios[i-1] {
				return fmt.Errorf("ratios must be asencing %d, %d not allowed", st.pRatios[i-1], st.pRatios[i])
			}
		}
		if _, ok := vals[ratio]; ok {
			return fmt.Errorf("ratio %d is duplicated.  No duplicate ratios allowed", ratio)
		}
	}
	return nil
}
=======
}
>>>>>>> scheduler algorithm and eval framework re-org
