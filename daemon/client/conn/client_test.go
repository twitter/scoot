package conn

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/scootdev/scoot/daemon/server"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	execeros "github.com/scootdev/scoot/runner/execer/os"
	"github.com/scootdev/scoot/runner/local"
	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

type testEnv struct {
	Server  server.Server
	Timeout time.Duration
	TempDir string
}

// test the connection to the server (using Echo)
// submit
func TestManyCommands(t *testing.T) {

	testEnv := setup()
	defer teardown(testEnv)

	timeout := testEnv.Timeout

	client := NewClientLib()

	var runIds []string
	var runIdMap map[string]string = make(map[string]string)

	// test the connection to the server with Echo
	echo, err := client.Echo("echoing")
	if err != nil {
		panic(fmt.Sprintf("echoing err:%s", err.Error()))
	}
	if strings.Compare("echoing", echo) != 0 {
		panic(fmt.Sprintf("expected 'echoing' from echo, got '%s'", echo))
	}

	// validate error messages in output's stderr
	//create an erroring snapshot - a snapshot with a script that errors
	errorSnapshotId := createSnapshot("erroringSnapshot", "#!/bin/sh\nbadCommand", client)

	// submit the erroring script  and validate that we get the correct output in the snapshot's stderr
	rId, runIds, runIdMap := assertRun([]string{"./script.sh"}, timeout, errorSnapshotId, "requesting erroring", runIds, runIdMap, client)
	statuses, err := client.Poll([]string{rId}, -1, false)
	if err != nil && strings.Compare("", err.Error()) != 0 {
		panic(fmt.Sprintf("poll for runids: %v, err: %s", runIds, err.Error()))
	}
	validateOutput("STDERR", "not found", statuses[0].SnapshotId, client)

	// validate messages in the output's stdout
	// submit the echo request and make sure the echo value is in the snapshot's stdout
	rId, runIds, runIdMap = assertRun([]string{"echo", "echo request"}, timeout, "", "requesting echo", runIds, runIdMap, client)
	statuses, err = client.Poll([]string{rId}, -1, false)
	if err != nil && strings.Compare("", err.Error()) != 0 {
		panic(fmt.Sprintf("poll for runids: %v, err: %s", runIds, err.Error()))
	}
	validateOutput("STDOUT", "echo request", statuses[0].SnapshotId, client)

	// validate a queue of 201 run requests complete
	// create a sleep snapshot - a snapshot with a script that sleeps for 0.5 seconds
	sleepSnapshotId := createSnapshot("sleepSnapshot", "#!/bin/sh\nsleep 0.5", client)

	// submit the sleep script request 1 times
	_, runIds, runIdMap = assertRun([]string{"./script.sh"}, timeout, sleepSnapshotId, "requesting sleep script", runIds, runIdMap, client)

	// submit the echo script request 200 times
	for i := 0; i < 200; i++ {
		msg := fmt.Sprintf("echo %d!!!", i)
		_, runIds, runIdMap = assertRun([]string{"echo", msg}, timeout, "", msg, runIds, runIdMap, client)
	}

	// verify that all finish: note: this should take ~5 seconds on a personal laptop.  Setting it to
	// 10 seconds in case travis is slow.  If it starts taking too long we can reduce the number of
	// tests in line 80.  Also we can speed up the test by commenting out the logging on the server side
	assertAllRunsFinish(runIds, runIdMap, 10*time.Second, client)

}

// run a command
func assertRun(cmd []string, timeout time.Duration, snapshotId string, errorTag string, runIds []string, runIdsMap map[string]string, client Client) (string, []string, map[string]string) {
	runId, err := client.Run(cmd, make(map[string]string), timeout, snapshotId)
	if err != nil && strings.Compare("", err.Error()) != 0 {
		panic(fmt.Sprintf("%s: runId:%s, err:%s", errorTag, runId, err.Error()))
	}

	runIds = append(runIds, runId)
	runIdsMap[runId] = ""
	return runId, runIds, runIdsMap

}

// validate the contents of the output snapshot
func validateOutput(outputFile string, expected string, snapshotId string, client Client) {

	// checkout the output snapshot
	tmpDir, err := temp.NewTempDir("", "checkout-")
	defer os.RemoveAll(tmpDir.Dir)
	client.CheckoutSnapshot(snapshotId, tmpDir.Dir)

	// validate that expected file is found with the expected contents
	file := fmt.Sprintf("%s%s%s", tmpDir.Dir, string(os.PathSeparator), outputFile)
	stderrMsg, err := ioutil.ReadFile(file)
	if err != nil {
		panic(fmt.Sprintf("error reading %s file:%s, error message:%s", outputFile, file, err.Error()))
	}

	if !strings.Contains(strings.ToLower(string(stderrMsg)), strings.ToLower(expected)) {
		panic(fmt.Sprintf("expected %s, got '%s'", expected, string(stderrMsg)))
	}
}

func assertAllRunsFinish(runIds []string, runIdMap map[string]string, timeout time.Duration, client Client) {
	start := time.Now().UnixNano()
	for {
		// did the assert timeout?
		now := time.Now()
		elapsedNs := time.Duration(now.UnixNano() - start)
		if elapsedNs.Nanoseconds() > timeout.Nanoseconds() {
			panic(fmt.Sprintf("test timed out waiting for runs to complete. %v are not done", runIds))
		}

		// get completed statuses
		statuses, err := client.Poll(runIds[:], -1, false)
		if err != nil && strings.Compare("", err.Error()) != 0 {
			panic(fmt.Sprintf("poll for runids: %v, err: %s", runIds, err.Error()))
		}

		// remove completed runs from the map
		for _, status := range statuses {
			delete(runIdMap, status.RunId)
		}

		// did everything finish?
		if len(runIdMap) == 0 {
			break // all runs have completed the test passed
		}

		// make a list of the remaining runIds
		runIds = make([]string, 0, len(runIdMap))
		for key, _ := range runIdMap {
			runIds = append(runIds, key)
		}
	}

}

// create a snapshot that has a script that sleeps for 0.5 seconds
func createSnapshot(errorTag string, scriptContents string, client Client) string {
	// create the script file in a temp dir
	dir, _ := temp.NewTempDir(os.TempDir(), "TempSnapshot")
	defer os.RemoveAll(dir.Dir)

	filename := fmt.Sprintf("%s%s%s", dir.Dir, string(os.PathSeparator), "script.sh")
	err := ioutil.WriteFile(filename, []byte(scriptContents), os.ModePerm)
	if err != nil {
		panic(fmt.Sprintf("%s: error %s. Creating file: %s, with contents %s", errorTag, err.Error(), filename, scriptContents))
	}

	// create the snapshot
	sId, err := client.CreateSnapshot(filename)
	if err != nil {
		panic(fmt.Sprintf("%s: error creating snapshot:%s", errorTag, err.Error()))
	}

	return sId
}

// setup the test environment:
// - set the timeout
// - create and start the server (using the queueingRunner)
func setup() testEnv {
	env := testEnv{Timeout: 1 * time.Second}

	flag.Parse()
	tempDir, err := temp.NewTempDir("", "scoot-listen-")
	if err != nil {
		panic(err)
	}
	env.TempDir = tempDir.Dir

	scootDir := path.Join(tempDir.Dir, "scoot")
	err = os.Setenv("SCOOTDIR", scootDir)

	// create the filer for the snapshots
	filerTmp, _ := temp.NewTempDir(os.TempDir(), "TestDaemonExample_filer")
	filer := snapshots.MakeTempFiler(filerTmp)

	h := server.NewHandler(getRunner(filer), filer, 50*time.Millisecond)
	env.Server, err = server.NewServer(h)
	if err != nil {
		panic(err)
	}

	l, _ := env.Server.Listen()
	go func() {
		env.Server.Serve(l)
	}()

	return env
}

func getRunner(filer snapshot.Filer) runner.Runner {
	ex := execeros.NewExecer()
	tempDir, err := temp.TempDirDefault()
	if err != nil {
		panic(err)
	}

	outputCreator, err := local.NewOutputCreator(tempDir)
	if err != nil {
		panic(err)
	}

	// create a simple runner as the queueing runner's delegate
	runnerAvailCh := make(chan struct{})
	simpleRunner := local.NewSimpleReportBackRunner(ex, filer, outputCreator, runnerAvailCh)

	return local.NewQueuingRunner(simpleRunner, 1000, runnerAvailCh)
}

func teardown(env testEnv) {
	server := env.Server
	server.Stop()

	os.RemoveAll(env.TempDir)

}
