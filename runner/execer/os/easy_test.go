package os

import (
	"bytes"
	"testing"
	"time"

	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/common/stats"
	scootexecer "github.com/twitter/scoot/runner/execer"

	log "github.com/sirupsen/logrus"
)

func init() {
	log.AddHook(hooks.NewContextHook())
	logrusLevel, _ := log.ParseLevel("debug")
	log.SetLevel(logrusLevel)
}

func TestAll(t *testing.T) {
	e := NewBoundedExecer(0, nil, nil)

	// TODO(dbentley): factor out an assertRun method
	cmd := scootexecer.Command{
		Argv: []string{"true"},
		LogTags: tags.LogTags{
			Tag:    "tag",
			JobID:  "jobID1234",
			TaskID: "taskID1234",
		},
	}
	p, err := e.Exec(cmd)
	if err != nil {
		t.Fatalf("Couldn't run true %v", err)
	}
	status := p.Wait()
	if status.State != scootexecer.COMPLETE || status.ExitCode != 0 {
		t.Fatalf("Got unexpected status running true %v", status)
	}

	cmd = scootexecer.Command{Argv: []string{"false"}}
	p, err = e.Exec(cmd)
	if err != nil {
		t.Fatalf("Couldn't run false %v", err)
	}
	status = p.Wait()
	if status.State != scootexecer.COMPLETE || status.ExitCode != 1 {
		t.Fatalf("Got unexpected status running false %v", status)
	}

}

func TestOutput(t *testing.T) {
	e := NewBoundedExecer(0, nil, nil)

	var stdout, stderr bytes.Buffer

	stdoutExpected := "hello world\n"
	// TODO(dbentley): factor out an assertRun method
	cmd := scootexecer.Command{
		Argv:   []string{"echo", "-n", stdoutExpected},
		Stdout: &stdout,
		Stderr: &stderr,
		LogTags: tags.LogTags{
			Tag:    "tag",
			JobID:  "jobID1234",
			TaskID: "taskID1234",
		},
	}
	p, err := e.Exec(cmd)
	if err != nil {
		t.Fatalf("Couldn't run true %v", err)
	}
	status := p.Wait()
	if status.State != scootexecer.COMPLETE || status.ExitCode != 0 {
		t.Fatalf("Got unexpected status running true %v", status)
	}
	stdoutText, stderrText := stdout.String(), stderr.String()
	if stdoutText != stdoutExpected || stderrText != "" {
		t.Fatalf("Incorrect output, got %q and %q; expected %q and \"\"",
			stdoutText, stderrText, stdoutExpected)
	}
}

func TestMemUsage(t *testing.T) {
	// Command to increase memory by 1MB every .1s until we hit 50MB after 5s.
	// Creates a bash process and under that a python process. They should both contribute to MemUsage.
	str := `import time; exec("x=[]\nfor i in range(50):\n x.append(' ' * 1024*1024)\n time.sleep(.1)")`
	cmd := scootexecer.Command{
		Argv: []string{"python3", "-c", str},
		LogTags: tags.LogTags{
			Tag:    "tag",
			JobID:  "jobID1234",
			TaskID: "taskID1234",
		},
	}
	e := NewBoundedExecer(0, nil, nil)
	p, err := e.Exec(cmd)
	if err != nil {
		t.Fatalf(err.Error())
	}
	p.(*process).ats = 1
	defer p.Abort()
	// Check for growing memory usage at [1.5, 3]s. Then check that the usage is between a reasonable min/max.
	prevUsage := 0
	sleepDuration := 500 * time.Millisecond
	for i := 0; i < 2; i++ {
		time.Sleep(sleepDuration)
		_, err := e.pw.GetProcs()
		if err != nil {
			t.Fatal(err)
		}
		if newUsage, err := e.pw.MemUsage(p.(*process).cmd.Process.Pid); err != nil {
			t.Fatalf(err.Error())
		} else if int(newUsage) <= prevUsage {
			t.Fatalf("Expected growing memory, got: %d -> %d @%dms", prevUsage, newUsage, (i+1)*int(sleepDuration/time.Millisecond))
		} else {
			prevUsage = int(newUsage)
		}
	}
	if prevUsage < 5*1024*1024 {
		t.Fatalf("Expected usage to be at least 5MB, was: %dB", prevUsage)
	}
	if prevUsage > 50*1024*1024 {
		t.Fatalf("Expected usage to be less than 50MB, was: %dB", prevUsage)
	}
}

func TestMemCap(t *testing.T) {
	// Command to increase memory by 1MB every .1s up to 5s.
	// Creates a bash process and under that a python process. They should both contribute to MemUsage.
	str := `import time; exec("x=[]\nfor i in range(50):\n x.append(' ' * 1024*1024)\n time.sleep(.1)")`
	memCh := make(chan scootexecer.ProcessStatus)
	cmd := scootexecer.Command{
		Argv: []string{"python3", "-c", str},
		LogTags: tags.LogTags{
			Tag:    "tag",
			JobID:  "jobID1234",
			TaskID: "taskID1234",
		},
		MemCh: memCh,
	}
	// Terminate nearly immediately, after memory grows to 1MB.
	e := NewBoundedExecer(scootexecer.Memory(1024*1024), nil, stats.NilStatsReceiver())
	p, err := e.Exec(cmd)
	if err != nil {
		t.Fatalf(err.Error())
	}
	p.(*process).ats = 1
	defer p.Abort()
	pid := p.(*process).cmd.Process.Pid
	var usage scootexecer.Memory
	var timeoutCh <-chan time.Time
	timeout := time.NewTimer(time.Second * 2)
	timeoutCh = timeout.C
	defer timeout.Stop()
	select {
	case <-memCh:
		_, err := e.pw.GetProcs()
		if err != nil {
			t.Fatal(err)
		}
		if usage, err = e.pw.MemUsage(pid); err != nil {
			// We don't return this error because it just means the process was already killed
			log.Errorf("Error finding memUsage: %s", err)
		}
		if usage != 0 {
			t.Fatalf("Expected usage to be 0MB, was: %dB", usage)
		}
	case <-timeoutCh:
		t.Fatalf("Memory usage didn't exceed memCap within 2 seconds")
	}
}
