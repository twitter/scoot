package loadtest

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/bazel/cas"
	"github.com/twitter/scoot/bazel/remoteexecution"
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/common/stats"
)

const (
	UploadLatency        = "write_latency"
	DownloadLatency      = "read_latency"
	BatchUploadLatency   = "batch_write_latency"
	BatchDownloadLatency = "batch_read_latency"
	KBYTE                = 1024
)

var TestDataSizes = [3]int{1, 10, 1000} // these sizes are 1kb units: 1kb, 10kb, 1m test files
var TestDataSizesStr = strings.Trim(strings.Join(strings.Fields(fmt.Sprint(TestDataSizes)), ","), "[]")

type StatusCode int

const (
	WaitingToStart StatusCode = iota
	Initializing
	InitUpload
	CreatingGoRoutines
	RunningActions
	PauseBetweenIterations
)

type Status struct {
	code StatusCode
	desc string
}

/*
ApiserverLoadTester is the object that runs the load test.  The RunLoadTest() function starts the load test.
*/
type ApiserverLoadTester struct {
	// cli args
	action         string // the test's action (upload, download, both)
	useBatchApi    bool   // use the CAS batch api
	minDataSetSize int    // the minimum data set size to use during the test
	maxDataSetSize int    // the maximium data set size to use during the test
	numActions     int    // the number of concurrent upload/downloads we want to trigger
	freq           int    // the frequency to repeat the test (0 = run once)
	totalTime      int    // the total elapsed time to allow repeating tests to run
	casGrpcAddr    string // the cas addr (<hostname>:<port>)

	// data set fields
	data                []byte   // the raw data set all uploads are derived from
	dataSizes           []int    // the list of data sizes to use on the test
	initUploadDigestIds []string // info on uploaded data sets for download testing
	completedCnt        int      // track number of completed test actions
	iterCnt             int      // track number of tests run (for freq != 0)
	killRequested       bool     // will be set to true if/when a kill is requested
	killReqeuestedMu    sync.RWMutex
	status              StatusCode
	batchContents       []cas.BatchUploadContent
	batchDigests        []*remoteexecution.Digest

	statsFile string

	// externals
	dialer *dialer.ConstantResolver
	casCli *cas.CASClient
	stat   stats.StatsReceiver

	// channels for goroutine coordination
	stopTestIterations chan bool
}

type Args struct {
	LogLevel    string
	Action      string
	DataSizeMin int
	DataSizeMax int
	NumTimes    int
	Freq        int
	TotalTime   int
	CasGrpcAddr string
	Batch       bool
}

func MakeApiserverLoadTester(a *Args) *ApiserverLoadTester {
	lt := ApiserverLoadTester{
		action:         a.Action,
		useBatchApi:    a.Batch,
		minDataSetSize: a.DataSizeMin,
		maxDataSetSize: a.DataSizeMax,
		numActions:     a.NumTimes,
		freq:           a.Freq,
		totalTime:      a.TotalTime,
		casGrpcAddr:    a.CasGrpcAddr,
		killRequested:  false,
		status:         WaitingToStart,
		iterCnt:        0,
	}

	// get the cas connection
	lt.dialer = dialer.NewConstantResolver(lt.casGrpcAddr)
	lt.casCli = cas.MakeCASClient()

	// initialize thes stats
	statsReceiver, _ := stats.NewCustomStatsReceiver(stats.NewFinagleStatsRegistry, 0)
	lt.stat = statsReceiver.Scope("cas_streaming")

	lt.stopTestIterations = make(chan bool)

	tdir := os.TempDir()
	_, err := os.Open(fmt.Sprintf("%sCloudExec", tdir))
	if os.IsNotExist(err) {
		os.Mkdir(fmt.Sprintf("%sCloudExec", tdir), 0777)
	}
	lt.statsFile = fmt.Sprintf("%sCloudExec/apiserver_load_test.csv", tdir)

	return &lt
}

/*
Start the load test
*/
func (lt *ApiserverLoadTester) RunLoadTest() error {
	/*
		if its a repeating test (freq > 0) run the first iteration of the test in this process, start a goroutine
		timing the frequency, then in this process loop running the next iterations when a timer sends a signal on its
		channel, or stopping when time is up or a kill request is received
	*/
	lt.status = Initializing

	_, err := os.Stat(lt.statsFile)
	if os.IsExist(err) {
		os.Remove(lt.statsFile)
	}

	// initialize the data sizes and data set for the test
	err = lt.initTestData()
	if err != nil {
		return fmt.Errorf("couldn't initialize the test data:%s", err.Error())
	}

	if lt.getKillRequested() {
		lt.drainStopChAndResetStatus()
		return nil
	}

	if lt.freq == 0 {
		lt.runOneIteration() // run the test once
		lt.drainStopChAndResetStatus()
		// reset for next test request
		lt.resetStatusToWaitingToStart()
		return nil
	}

	// run the test once
	lt.runOneIteration()
	if lt.getKillRequested() {
		lt.drainStopChAndResetStatus()
		return nil
	}

	// set up a timer to signal running the test every <freq> minutes
	tFreq := time.Duration(lt.freq) * time.Minute
	ticker := time.NewTicker(tFreq)
	go func() {
		time.Sleep(time.Duration(lt.totalTime) * time.Minute)
		ticker.Stop()
		if !lt.getKillRequested() { // if we already have killRequested, don't try to put stop signal on channel
			// stopTestIterations the test after totalTime
			lt.stopTestIterations <- true
		}
	}()

	// loop running the test on the timer signals until stop is received
	notDone := true
	for notDone {
		select {
		case <-lt.stopTestIterations:
			notDone = false
		case <-ticker.C:
			lt.runOneIteration()
		default:
			time.Sleep(2 * time.Second)
		}
	}

	// reset for next test request
	lt.resetStatusToWaitingToStart()
	return nil
}

func (lt *ApiserverLoadTester) getKillRequested() bool {
	lt.killReqeuestedMu.RLock()
	defer lt.killReqeuestedMu.RUnlock()
	return lt.killRequested
}

func (lt *ApiserverLoadTester) setKillRequested(val bool) {
	lt.killReqeuestedMu.Lock()
	defer lt.killReqeuestedMu.Unlock()
	lt.killRequested = val
}

func (lt *ApiserverLoadTester) resetStatusToWaitingToStart() {
	lt.status = WaitingToStart
	lt.setKillRequested(false)
	lt.iterCnt = 0
}

func (lt *ApiserverLoadTester) drainStopChAndResetStatus() {
	select {
	case <-lt.stopTestIterations: // drain the lt.stopTestIterations channel
	default:
	}
	log.Infof("kill request channel was drained")
	lt.resetStatusToWaitingToStart()
}

func (lt *ApiserverLoadTester) runOneIteration() {
	// create numActions goroutines all waiting for a start request.  Each goroutine will run an action
	// using one of the test files from the prior step
	lt.stat.Render(false) // clear the stats
	startCh := make(chan struct{})
	actionDoneCh := make(chan int, lt.numActions)
	allDoneCh := make(chan struct{})

	log.Infof("starting waiting go routines")
	// create all goroutines waiting for start action
	lt.status = CreatingGoRoutines
	lt.completedCnt = 0
	lt.batchContents = make([]cas.BatchUploadContent, 0)
	lt.batchDigests = make([]*remoteexecution.Digest, 0)
	for i := 0; i < lt.numActions; i++ {
		if lt.getKillRequested() {
			break
		}
		dIdx := 0 // select the data set to use
		if len(lt.initUploadDigestIds) > 1 {
			dIdx = int(rand.Float32() * float32(len(lt.initUploadDigestIds)))
		}
		if lt.useBatchApi {
			lt.accumulateBatchContent(dIdx)
			if i == lt.numActions-1 {
				go lt.performBatchAction(startCh, actionDoneCh)
			}
		} else {
			go lt.performTestAction(dIdx, startCh, actionDoneCh)
		}
	}
	// create go routine collecting done count
	go lt.collectFinishActions(actionDoneCh, allDoneCh)

	log.Infof("triggering the go routines to start uploads/downloads")
	close(startCh) // signal start all actions
	lt.status = RunningActions

	<-allDoneCh // wait for all actions to be finished
	lt.status = PauseBetweenIterations
	lt.iterCnt++

	lt.writeStatsToFile()
}

func (lt *ApiserverLoadTester) writeStatsToFile() {
	statsJson := lt.stat.Render(false) // get the stats (but don't reset in case we get status request)

	// convert to comma delimited string
	statsMap := make(map[string]interface{})
	json.Unmarshal([]byte(statsJson), &statsMap) // make into a map (statsMap)

	// sort the map to print in same order each time
	keys := make([]string, 0)
	for k := range statsMap {
		keys = append(keys, k)
	}
	line := new(bytes.Buffer)
	// add test params
	now := time.Now().Format("2006-01-02 15:04:05 MST")
	fmt.Fprintf(line, "%s,action,%s,batch,%t,num_times,%d,min_size,%d,max_size,%d, ", now,
		lt.action, lt.useBatchApi, lt.numActions, lt.minDataSetSize, lt.maxDataSetSize)
	sort.Strings(keys)
	for _, sKey := range keys {
		fmt.Fprintf(line, "%s, %v,", sKey, statsMap[sKey])
	}
	fmt.Fprintf(line, "\n")

	// append to the file
	f, _ := os.OpenFile(lt.statsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	defer f.Close()
	f.Write(line.Bytes())
	log.Infof("stats written to %s", lt.statsFile)
}

func (lt *ApiserverLoadTester) collectFinishActions(oneActionDoneCh chan int, allDoneCh chan struct{}) {
	// collect finished actions

	stopLooping := false
	numActions := lt.numActions
	if lt.useBatchApi {
		numActions = 1
	}
	for cnt := 0; cnt < numActions && !stopLooping; {
		select {
		case completed := <-oneActionDoneCh:
			lt.completedCnt += completed
			cnt++
		default:
			if lt.getKillRequested() {
				stopLooping = true
			}
		}
	}
	log.Infof("collected %d done signals", lt.completedCnt)
	close(allDoneCh)
	log.Infof("done collecting finish actions")
	// signal all actions are done
}

// wait for the start signal, then perform the test action
func (lt *ApiserverLoadTester) performTestAction(dataIdx int, startCh chan struct{}, doneCh chan int) {

	<-startCh
	var err error
	if lt.action == "upload" {
		_, err = lt.uploadADataSet(lt.dataSizes[dataIdx])
	} else if lt.action == "download" {
		err = lt.downloadAFile(lt.initUploadDigestIds[dataIdx])
	} else { // action is "both"
		if rand.Float32() > 0.5 {
			_, err = lt.uploadADataSet(lt.dataSizes[dataIdx])
		} else {
			err = lt.downloadAFile(lt.initUploadDigestIds[dataIdx])
		}
	}
	if err != nil {
		log.Errorf("%s", err.Error())
		doneCh <- 0
	} else {
		doneCh <- 1
	}
}

func (lt *ApiserverLoadTester) performBatchAction(startCh chan struct{}, doneCh chan int) error {
	<-startCh
	if lt.action == "upload" {
		log.Debugf("uploading:%d entries", len(lt.batchContents))
		defer lt.stat.Latency(BatchUploadLatency).Time().Stop()
		_, err := lt.casCli.BatchUpdateWrite(lt.dialer, lt.batchContents,
			backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5))
		if err != nil {
			doneCh <- 0
			return fmt.Errorf("batch upload error:%s", err.Error())
		}
	} else {
		log.Debugf("downloading:%d entries", len(lt.batchContents))
		defer lt.stat.Latency(BatchDownloadLatency).Time().Stop()
		_, err := lt.casCli.BatchRead(lt.dialer, lt.batchDigests,
			backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5))
		if err != nil {
			doneCh <- 0
			return fmt.Errorf("batch download error:%s", err.Error())
		}
	}
	doneCh <- 1
	return nil
}

func (lt *ApiserverLoadTester) accumulateBatchContent(dataIdx int) error {
	var digest *remoteexecution.Digest
	var theData []byte
	if lt.action == "upload" {
		theData, digest = lt.makeUploadContent(lt.dataSizes[dataIdx])
		lt.batchContents = append(lt.batchContents, cas.BatchUploadContent{
			Digest: digest,
			Data:   theData,
		})
	} else {
		digest = &remoteexecution.Digest{
			Hash:      lt.initUploadDigestIds[dataIdx],
			SizeBytes: int64(lt.dataSizes[dataIdx]),
		}
		lt.batchDigests = append(lt.batchDigests, digest)
	}

	if lt.getKillRequested() {
		return fmt.Errorf("Kill request received, batch upload skipped")
	}

	return nil
}

func (lt *ApiserverLoadTester) uploadADataSet(numKBytes int) (string, error) {

	// make the upload content unique: update the first KBYTE bytes of the common data set with a random set of values
	theData, digest := lt.makeUploadContent(numKBytes)
	if lt.getKillRequested() {
		return "", fmt.Errorf("Kill request received, upload skipped")
	}
	log.Debugf("uploading:%v", digest)
	defer lt.stat.Latency(UploadLatency).Time().Stop()
	err := lt.casCli.ByteStreamWrite(lt.dialer, digest, theData[:],
		backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5))
	if err != nil {
		return "", fmt.Errorf("Upload error:%s", err.Error())
	}
	return digest.Hash, nil
}

// make a unique data set and digest for it
func (lt *ApiserverLoadTester) makeUploadContent(numKBytes int) ([]byte, *remoteexecution.Digest) {
	size := int64(numKBytes * KBYTE)
	theData := make([]byte, 0)
	uniqPref := lt.makeDummyData(KBYTE)
	theData = append(theData, uniqPref[:]...)
	if size > KBYTE {
		theData = append(theData, lt.data[0:size-KBYTE]...)
	}
	t := sha256.Sum256(theData)
	dataSha := fmt.Sprintf("%x", t)
	// create the upload request data structure
	digest := &remoteexecution.Digest{Hash: dataSha, SizeBytes: size}
	return theData, digest
}

func (lt *ApiserverLoadTester) downloadAFile(digestId string) error {
	digest := &remoteexecution.Digest{Hash: digestId}

	if lt.getKillRequested() {
		return fmt.Errorf("Kill request received, download skipped")
	}
	log.Debugf("downloading:%v", digest)
	defer lt.stat.Latency(DownloadLatency).Time().Stop()
	_, err := lt.casCli.ByteStreamRead(lt.dialer, digest,
		backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5))
	if err != nil {
		return fmt.Errorf("Error downloading id:%s.  Err:%s", digestId, err.Error())
	}
	return nil
}

// create a random data set
func (lt *ApiserverLoadTester) makeDummyData(size int) []byte {
	rand.Seed(time.Now().UnixNano())
	data := make([]byte, size)
	rand.Read(data)
	return data
}

// fill the dataSizes array in ApiserverLoadTester with the data sizes selected for the test
func (lt *ApiserverLoadTester) initTestData() error {

	// get data set sizes from cli min,max range
	j := 0
	for i := 0; i < len(TestDataSizes); i++ {
		if TestDataSizes[i] >= lt.minDataSetSize && TestDataSizes[i] <= lt.maxDataSetSize {
			j++
		}
	}
	lt.dataSizes = make([]int, j)
	j = 0
	for i := 0; i < len(TestDataSizes); i++ {
		if TestDataSizes[i] >= lt.minDataSetSize && TestDataSizes[i] <= lt.maxDataSetSize {
			lt.dataSizes[j] = TestDataSizes[i]
			j++
		}
	}

	// create the common data set containing the max data the testing needs
	lt.data = lt.makeDummyData(lt.dataSizes[len(lt.dataSizes)-1] * KBYTE) // assume TestDataSizes in ascending order

	if lt.action == "download" || lt.action == "both" {
		lt.status = InitUpload
		lt.initUploadDigestIds = make([]string, len(lt.dataSizes))
		for i := 0; i < len(lt.dataSizes); i++ {
			// upload the data so it is available for download
			digestId, err := lt.uploadADataSet(lt.dataSizes[i])
			if err != nil {
				return fmt.Errorf("Couldn't upload the %d sized initial data set:%s", lt.dataSizes[i], err.Error())
			}
			lt.initUploadDigestIds[i] = digestId
		}
	}

	return nil
}

func (lt *ApiserverLoadTester) getStatsReceiver() stats.StatsReceiver {
	return lt.stat
}

/*
GetStatus returns the current state of the test.
*/
func (lt *ApiserverLoadTester) GetStatus() Status {
	switch lt.status {

	case Initializing:
		return Status{Initializing, fmt.Sprintf("Initializing, iteration %d", lt.iterCnt)}
	case InitUpload:
		return Status{InitUpload, fmt.Sprintf("Iteration %d: Uploading data sets for downloads.", lt.iterCnt)}
	case CreatingGoRoutines:
		return Status{CreatingGoRoutines, fmt.Sprintf("Iteration %d: Creating goroutines to run the test actions.", lt.iterCnt)}
	case RunningActions:
		return Status{RunningActions, fmt.Sprintf("Iteration %d: Running actions %d of %d have finished.",
			lt.iterCnt, lt.completedCnt, lt.numActions)}
	case PauseBetweenIterations:
		return Status{PauseBetweenIterations, fmt.Sprintf("Iteration %d: Pause after iteration.", lt.iterCnt)}
	default:
		stats, err := ioutil.ReadFile(lt.statsFile)
		if err != nil {
			return Status{code: WaitingToStart, desc: fmt.Sprintf("%s", err.Error())}
		}
		return Status{code: WaitingToStart, desc:string(stats)}
	}
}

func (s Status) String() string {
	return fmt.Sprintf("{'code': %d; 'desc':'%s'}", s.code, s.desc)
}

/*
Trigger kill action.
*/
func (lt *ApiserverLoadTester) KillTest() {
	if lt.status != WaitingToStart {
		lt.setKillRequested(true)
		lt.stopTestIterations <- true
	}
	lt.stat.Render(false) // clear the stats registry
}
