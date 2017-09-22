package server

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"testing"
	"time"

	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/config/jsonconfig"
	"github.com/twitter/scoot/ice"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer"
	"github.com/twitter/scoot/runner/execer/execers"
	"github.com/twitter/scoot/runner/runners"
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/git/repo"
	"github.com/twitter/scoot/workerapi/gen-go/worker"
)

/*
Test the stats collected by the server's stats() goroutine:
*/
func TestInitStats(t *testing.T) {

	//setup the test environment
	// create a worker - (starting the init activity)
	h, initDoneCh, statsRegistry, simExecer := setupTestEnv(false)

	time.Sleep(stats.StatReportIntvl + (10 * time.Millisecond))

	// verify stats during initialization
	if !stats.StatsOk("validating worker still initing stats ", statsRegistry, t,
		map[string]stats.Rule{
			fmt.Sprintf("handler/%s", stats.WorkerServerStartedGauge):           {Checker: stats.Int64EqTest, Value: 1},
			fmt.Sprintf("handler/%s", stats.WorkerFinalInitLatency_ms):          {Checker: stats.DoesNotExistTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerActiveInitLatency_ms):         {Checker: stats.Int64GTTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerActiveRunsGauge):              {Checker: stats.DoesNotExistTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerFailedCachedRunsGauge):        {Checker: stats.DoesNotExistTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerTimeSinceLastContactGauge_ms): {Checker: stats.DoesNotExistTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerUptimeGauge_ms):               {Checker: stats.DoesNotExistTest, Value: 0},
		}) {
		t.Fatal("init stats test failed")
	}

	initDoneCh <- nil // trigger end of initialization

	time.Sleep(2*stats.StatReportIntvl + (20 * time.Millisecond))
	// verify stats after initialization
	if !stats.StatsOk("validating worker done initing stats ", statsRegistry, t,
		map[string]stats.Rule{
			fmt.Sprintf("handler/%s", stats.WorkerFinalInitLatency_ms):          {Checker: stats.Int64GTTest, Value: 109},
			fmt.Sprintf("handler/%s", stats.WorkerActiveInitLatency_ms):         {Checker: stats.Int64EqTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerActiveRunsGauge):              {Checker: stats.Int64EqTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerFailedCachedRunsGauge):        {Checker: stats.Int64EqTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerTimeSinceLastContactGauge_ms): {Checker: stats.Int64GTTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerUptimeGauge_ms):               {Checker: stats.Int64GTTest, Value: 0},
		}) {
		t.Fatal("init done stats test failed")
	}

	// trigger a pausing command
	runCmd := &worker.RunCommand{Argv: []string{"pause", "complete 0"}}
	h.Run(runCmd)

	time.Sleep(stats.StatReportIntvl + (10 * time.Millisecond))
	// verify stats during paused command
	if !stats.StatsOk("validating command running stats ", statsRegistry, t,
		map[string]stats.Rule{
			fmt.Sprintf("handler/%s", stats.WorkerFinalInitLatency_ms):          {Checker: stats.Int64GTTest, Value: 109},
			fmt.Sprintf("handler/%s", stats.WorkerActiveInitLatency_ms):         {Checker: stats.Int64EqTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerActiveRunsGauge):              {Checker: stats.Int64EqTest, Value: 1},
			fmt.Sprintf("handler/%s", stats.WorkerFailedCachedRunsGauge):        {Checker: stats.Int64EqTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerTimeSinceLastContactGauge_ms): {Checker: stats.Int64GTTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerUptimeGauge_ms):               {Checker: stats.Int64GTTest, Value: 0},
		}) {
		t.Fatal("init done stats test failed")
	}

	// let the command finish
	simExecer.Resume()
	// verify stats after command is done
	time.Sleep(stats.StatReportIntvl + (10 * time.Millisecond))
	// verify stats during paused command
	if !stats.StatsOk("validating command running stats ", statsRegistry, t,
		map[string]stats.Rule{
			fmt.Sprintf("handler/%s", stats.WorkerFinalInitLatency_ms):          {Checker: stats.Int64GTTest, Value: 109},
			fmt.Sprintf("handler/%s", stats.WorkerActiveInitLatency_ms):         {Checker: stats.Int64EqTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerActiveRunsGauge):              {Checker: stats.Int64EqTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerFailedCachedRunsGauge):        {Checker: stats.Int64EqTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerTimeSinceLastContactGauge_ms): {Checker: stats.Int64GTTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerUptimeGauge_ms):               {Checker: stats.Int64GTTest, Value: 0},
		}) {
		t.Fatal("init done stats test failed")
	}
}

/*
Test the stats collected by the server's stats() goroutine:
*/
func TestFailedRunsStats(t *testing.T) {

	//setup the test environment
	// create a worker - (starting the init activity)
	h, initDoneCh, statsRegistry, simExecer := setupTestEnv(true)
	time.Sleep(stats.StatReportIntvl + (10 * time.Millisecond))

	initDoneCh <- nil // trigger end of initialization
	time.Sleep(stats.StatReportIntvl + (10 * time.Millisecond))

	runCmd := &worker.RunCommand{Argv: []string{"pause", "complete 0"}}
	h.Run(runCmd)
	time.Sleep(stats.StatReportIntvl + (10 * time.Millisecond))

	simExecer.Resume()
	// verify stats after command is done
	time.Sleep(stats.StatReportIntvl + (10 * time.Millisecond))
	// verify stats during paused command
	if !stats.StatsOk("validating command running stats ", statsRegistry, t,
		map[string]stats.Rule{
			fmt.Sprintf("handler/%s", stats.WorkerActiveRunsGauge):       {Checker: stats.Int64EqTest, Value: 0},
			fmt.Sprintf("handler/%s", stats.WorkerFailedCachedRunsGauge): {Checker: stats.Int64EqTest, Value: 1},
		}) {
		t.Fatal("init done stats test failed")
	}
}

func setupTestEnv(useErrorExec bool) (h *handler, initDoneCh chan error, statsRegistry stats.StatsRegistry, simExecer *execers.SimExecer) {

	stats.StatReportIntvl = 100 * time.Millisecond
	log.AddHook(hooks.NewContextHook())

	//use initDoneCh to control the initialization latency
	initDoneCh = make(chan error, 1)
	statsRegistry = stats.NewFinagleStatsRegistry()
	simExecer = execers.NewSimExecer()
	tmpDir, err := temp.TempDirDefault()
	configText := "{}"

	bag := ice.NewMagicBag()
	schema := jsonconfig.EmptySchema()
	bag.InstallModule(temp.Module())
	bag.InstallModule(runners.Module())
	bag.InstallModule(Module())
	bag.PutMany(
		func() execer.Execer {
			return simExecer
		},
		func(db snapshot.DB) snapshot.Filer {
			return snapshot.NewDBAdapter(db)
		},
		runners.NewSingleRunner,
		func() snapshot.InitDoneCh {
			return initDoneCh
		},
		// don't have the fake db pause during any of the tests.  Use an externally
		// visible channel if the tests need to control the duration of db operations
		func() dbPauseCh {
			return nil
		},
		makePausingNoopDb,
		func() stats.StatsReceiver {
			statsRec, _ := stats.NewCustomStatsReceiver(func() stats.StatsRegistry { return statsRegistry }, 0)
			return statsRec
		},
		func(stat stats.StatsReceiver, run runner.Service) worker.Worker {
			return NewHandler(stat, run)
		},
	)
	if useErrorExec {
		bag.Put(
			makeNoopOutputCreator,
		)
	} else {
		bag.Put(
			func() runner.OutputCreator {
				oc, _ := runners.NewHttpOutputCreator(tmpDir, "")
				return oc
			},
		)
	}
	log.Info("workerapi/server RunServer(), config is:", configText)
	// Parse Config
	mod, err := schema.Parse([]byte(configText))
	if err != nil {
		log.Fatal("Error configuring Worker: ", err)
	}

	// Initialize Objects Based on Config Settings
	bag.InstallModule(mod)

	// get the handler
	var w worker.Worker
	err = bag.Extract(&w)
	if err != nil {
		log.Fatal("Error getting server", err)
	}

	h = w.(*handler)

	return

}

// ************************ fake objects for tests:  (do we already have these somewhere?)
type dbPauseCh chan interface{}

type pausingDB struct {
	waitCh dbPauseCh
}

func makePausingNoopDb(waitCh dbPauseCh) snapshot.DB {
	return &pausingDB{waitCh: waitCh}
}

func (pdb *pausingDB) wait() {
	if pdb.waitCh != nil {
		<-pdb.waitCh
	}
}
func (pdb *pausingDB) IngestDir(dir string) (snapshot.ID, error) {
	pdb.wait()
	return "nilSnapshoId", nil
}
func (pdb *pausingDB) IngestGitCommit(ingestRepo *repo.Repository, commitish string) (snapshot.ID, error) {
	pdb.wait()
	return "nilSnapshoId", nil
}
func (pdb *pausingDB) IngestGitWorkingDir(ingestRepo *repo.Repository) (snapshot.ID, error) {
	pdb.wait()
	return "nilSnapshoId", nil
}
func (pdb *pausingDB) ReadFileAll(id snapshot.ID, path string) ([]byte, error) {
	pdb.wait()
	return []byte{}, nil
}
func (pdb *pausingDB) Checkout(id snapshot.ID) (path string, err error) {
	pdb.wait()
	return "", nil
}
func (pdb *pausingDB) ReleaseCheckout(path string) error {
	pdb.wait()
	return nil
}
func (pdb *pausingDB) ExportGitCommit(id snapshot.ID, exportRepo *repo.Repository) (commit string, err error) {
	pdb.wait()
	return "", nil
}
func (pdb *pausingDB) Update() error {
	pdb.wait()
	return nil
}
func (pdb *pausingDB) UpdateInterval() time.Duration {
	return time.Millisecond * 100
}

type erroringOutputCreator struct{}

func makeNoopOutputCreator() runner.OutputCreator {
	return erroringOutputCreator{}
}
func (noopOc erroringOutputCreator) Create(id string) (runner.Output, error) {
	return noopOutput{}, nil
}

type noopOutput struct{}

func (noo noopOutput) URI() string {
	return ""
}
func (noo noopOutput) AsFile() string {
	return ""
}
func (noo noopOutput) Close() error {
	return nil
}
func (noo noopOutput) Write(p []byte) (n int, err error) {
	return 0, nil
}
