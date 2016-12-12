package integration_py

import (
	"flag"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/scootdev/scoot/daemon/server"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	os_execers "github.com/scootdev/scoot/runner/execer/os"
	"github.com/scootdev/scoot/runner/local"
	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

var s *server.Server

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

func TestPython(t *testing.T) {
	cmd := exec.Command("../../protocol/python/tests/integration.py")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("TestPython failed: %v", err)
	}
}

func TestPythonCli(t *testing.T) {
	cmd := exec.Command("../../protocol/python/tests/daemon_cli_test.py")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("TestPythonCli failed: %v", err)
	}
}

func TestRunMany(t *testing.T) {
	cmd := exec.Command("../../protocol/python/tests/run_many.py")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("TestPythonCli failed: %v", err)
	}
}

func getRunner(filer snapshot.Filer) runner.Runner {
	ex := os_execers.NewExecer()

	tempDir, err := temp.TempDirDefault()
	if err != nil {
		panic(err)
	}

	outputCreator, err := local.NewOutputCreator(tempDir, nil)
	if err != nil {
		panic(err)
	}

	return local.NewSimpleRunner(ex, filer, outputCreator)
}
