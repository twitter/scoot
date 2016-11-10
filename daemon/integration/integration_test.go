package integration_test

// this integration test exercises the cli
// TODO rename to cli_integration_test.go ? and extend with
// createSnapshot, Poll, checkoutSnapshot, when these have been added to the cli
// perhaps just reuse client_test.go - refactor it to either go to cli or directly
// to client_lib
import (
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/scootdev/scoot/daemon/client/cli"
	"github.com/scootdev/scoot/daemon/client/conn"
	"github.com/scootdev/scoot/daemon/integration"
	"github.com/scootdev/scoot/daemon/server"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer/execers"
	"github.com/scootdev/scoot/runner/local"
	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

var s *server.Server

func TestEcho(t *testing.T) {
	stdout, _, err := run("echo", "foo")
	if err != nil {
		t.Fatalf("error echo'ing: %v", err)
	}
	if stdout != "foo\n" {
		t.Fatalf("Echo didn't echo foo: %q", stdout)
	}
}

func TestRunSimpleCommand(t *testing.T) {
	// run the command
	var statusReq []string = []string{"run", "complete 0"}
	runId := assertRun("first run", "nil", statusReq[:], t)

	waitForStateWithTicker("TestRunSimpleCommand: first wait", runId, "complete", 3*time.Second, t)
}

// send 2 run commands, where the first one sleeps for a short while to block the second one
// expected results: first request runs, second request returns an error
// TODO when the server uses queueing runner, fix the 2nd request to expect queue, not rejection
func TestRun2Commands(t *testing.T) {
	// run the first command
	var statusReq []string = []string{"run", "sleep 1", "complete 2"}
	runId := assertRun("first run", "nil", statusReq[:], t)

	// run the second command, it should get runner busy message
	statusReq = []string{"run", "complete 3"}
	assertRun("second run", "nil", statusReq[:], t)

	waitForStateWithTicker("TestRun2Commands: first wait", runId, "complete", 3*time.Second, t)
}

func assertRun(tag string, errSubstring string, runArgs []string, t *testing.T) string {
	stdout, _, err := run(runArgs[0:]...)

	if strings.Compare(errSubstring, "nil") != 0 {
		if err == nil {
			panic(fmt.Sprintf("%s: Run(%v) failed, expected err to be %s, got: nil", tag, runArgs, errSubstring))
		} else if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(errSubstring)) {
			panic(fmt.Sprintf("%s: Run(%v) failed, expected to find '%s' in err, got: '%s'", tag, runArgs, errSubstring, err.Error()))
		}
	} else if err != nil {
		panic(fmt.Sprintf("%s: Run(%v) failed, expected err to be nil, got: %s", tag, runArgs, err.Error()))
	}

	return stdout
}

func run(args ...string) (string, string, error) {
	dialer, err := conn.UnixDialer()
	if err != nil {
		return "", "", err
	}

	cl, err := cli.NewCliClient(conn.NewCachingDialer(dialer))
	if err != nil {
		return "", "", err
	}
	defer cl.Close()
	return integration.Run(cl, args...)
}

// wait function that uses sleep - for comparing code complexity to using ticker and timer
// (see function below)
func waitForState(tag string, runId string, expectedStatus string, timeout time.Duration, t *testing.T) {

	start := time.Now().UnixNano()

	extendedTag := fmt.Sprintf("%s: %s", "waiting", tag)

	for {
		// get the status
		statusReq := []string{"status", runId}
		status := assertRun(extendedTag, "nil", statusReq[:], t)

		if strings.Compare(strings.ToLower(expectedStatus), strings.ToLower(status)) == 0 {
			return
		}

		// timeout?
		now := time.Now()
		elapsedNs := time.Duration(now.UnixNano() - start)
		if elapsedNs < 0 || elapsedNs.Nanoseconds() > timeout.Nanoseconds() {
			panic(fmt.Sprintf("%s: test timed out waiting for state %s on runid %s", tag, expectedStatus, runId))
		}

		// stall 10ms
		time.Sleep(100 * time.Millisecond)
	}

}

// wait function that uses a ticker and timer
func waitForStateWithTicker(tag string, runId string, expectedStatus string, timeout time.Duration, t *testing.T) {

	extendedTag := fmt.Sprintf("%s, %s", "waiting", tag)

	statusCheckTicker := time.NewTicker(100 * time.Millisecond)
	timeouter := &time.Timer{}
	if timeout > 0 {
		timeouter = time.NewTimer(timeout)
		defer timeouter.Stop()
	} else {
		panic("In waiting: Timeout must be > 0")
	}
	defer statusCheckTicker.Stop()

	// Helper that loops over the provided runIds and stores a status if it's finished or if all==true.
	// Returns true if any of the status entries are finished.
	isStatus := func(tag string) (bool, string) {
		statusReq := []string{"status", runId}
		status := assertRun(extendedTag, "nil", statusReq[:], t)

		if strings.Compare(strings.ToLower(expectedStatus), strings.ToLower(status)) == 0 {
			return true, status
		}
		return false, status
	}

	for {
		select {
		case <-timeouter.C:
			if ok, status := isStatus(extendedTag); !ok {
				panic(fmt.Sprintf("%s: Timeout expired, run in state: %s", tag, status))
			}
		case <-statusCheckTicker.C:
			isStatus, _ := isStatus(extendedTag)
			if isStatus {
				return
			}
		}
	}

}

func TestMain(m *testing.M) {
	flag.Parse()
	tempDir, err := temp.NewTempDir("", "scoot-listen-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tempDir.Dir)

	scootDir := path.Join(tempDir.Dir, "scoot")
	err = os.Setenv("SCOOTDIR", scootDir)

	filer := snapshots.MakeTempFiler(tempDir)
	h := server.NewHandler(getRunner(filer), filer, 50*time.Millisecond)
	s, err := server.NewServer(h)
	if err != nil {
		panic(err)
	}

	l, _ := s.Listen()
	go func() {
		s.Serve(l)
	}()

	defer s.Stop()

	os.Exit(m.Run())
}

func getRunner(filer snapshot.Filer) runner.Runner {
	ex := execers.NewSimExecer()
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
