package integration_test

import (
	"flag"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
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
	"github.com/scootdev/scoot/snapshot/snapshots"
)

// TODO prefer deterministic sequencing vs sleep

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
	runId := assertRun("ignore", "running", "nil", statusReq[:], t)

	// give the command time to finish running
	time.Sleep(100 * time.Millisecond)

	// get the status
	statusReq = []string{"status", runId}
	assertRun("complete", "ignore", "nil", statusReq[:], t)
}

// send 2 run commands, where the first one sleeps for a short while to block the second one
// expected results: first request runs, second request returns an error
// TODO when the server uses queueing runner, fix the 2nd request to expect queue, not rejection
func TestRun2Commands(t *testing.T) {
	// run the first command
	var statusReq []string = []string{"run", "sleep 1000"}
	runId := assertRun("ignore", "running", "nil", statusReq[:], t)

	// run the second command, it should get runner busy message
	statusReq = []string{"run", "complete 0"}
	assertRun("ignore", "ignore", local.RunnerBusyMsg, statusReq[:], t)

	// wait for the command to finish running
	time.Sleep(1500 * time.Millisecond)

	// get the status of the first run
	statusReq = []string{"status", runId}

	assertRun("complete", "ignore", "nil", statusReq[:], t)
}

func assertRun(expectedStdout, stderrSubstring, errSubstring string, runArgs []string, t *testing.T) string {
	stdout, stderr, err := run(runArgs[0:]...)

	if strings.Compare(errSubstring, "nil") != 0 {
		if err == nil {
			t.Fatalf("Run(%v) failed, expected err to be %s, got: nil", runArgs, errSubstring)
		} else if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(errSubstring)) {
			t.Fatalf("Run(%v) failed, expected to find '%s' in err, got: '%s'", runArgs, stderrSubstring, stderr)
		}
	} else if err != nil {
		t.Fatalf("Run(%v) failed, expected err to be nil, got: %s", runArgs, err.Error())
	}

	if strings.Compare(expectedStdout, "ignore") != 0 && strings.Compare(strings.ToLower(stdout), strings.ToLower(expectedStdout)) != 0 {
		t.Fatalf("Run(%v) failed, expected stdout to be '%s', got '%s'", runArgs, expectedStdout, stdout)
	}

	if strings.Compare(stderrSubstring, "ignore") != 0 && len(stderrSubstring) != 0 && !strings.Contains(strings.ToLower(stderr), strings.ToLower(stderrSubstring)) {
		t.Fatalf("Run(%v) failed, expected to find '%s' in stderr, got: '%s'", runArgs, stderrSubstring, stderr)
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

func TestMain(m *testing.M) {
	flag.Parse()
	tempDir, err := ioutil.TempDir("", "scoot-listen-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tempDir)

	scootDir := path.Join(tempDir, "scoot")
	err = os.Setenv("SCOOTDIR", scootDir)

	s, err = server.NewServer(getRunner())
	if err != nil {
		panic(err)
	}

	l, err := server.Listen()
	go func() {
		s.Serve(l)
	}()

	defer s.Stop()

	os.Exit(m.Run())
}

//TODO update this to use queuing runner
func getRunner() runner.Runner {
	wg := &sync.WaitGroup{}
	ex := execers.NewSimExecer(wg)
	tempDir, err := temp.TempDirDefault()
	if err != nil {
		panic(err)
	}

	outputCreator, err := local.NewOutputCreator(tempDir)
	if err != nil {
		panic(err)
	}
	return local.NewSimpleRunner(ex, snapshots.MakeInvalidCheckouter(), outputCreator)
}
