package exec

import (
	"bytes"
	"errors"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	outputExitScript string = `
#!/bin/bash
echo "stdout line"
echo "stderr line" 1>&2`
	noTrapScript string = `
#!/bin/bash
while :
do sleep 1
done`
	trapScript string = `
#!/bin/bash
trap ':' SIGTERM
while :
do sleep 1
done`
)

func TestUnrunnableCommand(t *testing.T) {
	cmd := NewOsExec().Command("sjkldoeiujeiuc")
	rr := RunKillableCommand(cmd, nil, 0*time.Second, ioutil.Discard, 0)
	if rr.Error == nil {
		t.Fatal("unexpected nil error from unrunnable command")
	}
}

func TestRunKillableCommandOutput(t *testing.T) {
	tf, err := setupTempScript(outputExitScript)
	if err != nil {
		t.Fatalf("failed setting up temp script file: %s", err)
	}
	defer os.Remove(tf.Name())

	var stream bytes.Buffer
	cmd := NewOsExec().Command("/bin/bash", tf.Name())
	rr := RunKillableCommand(cmd, nil, 0*time.Second, &stream, 0)
	if rr.Error != nil {
		t.Fatalf("error running command: %s", rr.Error)
	}
	if !rr.ProcessState.Exited() || rr.ProcessState.ExitCode() != 0 {
		t.Fatalf("process didn't complete successfully: %t %d", rr.ProcessState.Exited(), rr.ProcessState.ExitCode())
	}
	if !bytes.Contains(rr.Stdout, []byte("stdout line")) {
		t.Fatalf("expected output in stdout not present. stdout: %s", rr.Stdout)
	}
	if !bytes.Contains(rr.Stderr, []byte("stderr line")) {
		t.Fatalf("expected output in stderr not present. stderr: %s", rr.Stderr)
	}
	if !bytes.Contains(stream.Bytes(), []byte("stdout line")) || !bytes.Contains(stream.Bytes(), []byte("stderr line")) {
		t.Fatalf("expected out and err lines in stream not present. stream: %s", stream.Bytes())
	}
}

func TestRunKillableCommandSigterm(t *testing.T) {
	tf, err := setupTempScript(noTrapScript)
	if err != nil {
		t.Fatalf("failed setting up temp script file: %s", err)
	}
	defer os.Remove(tf.Name())

	// close channel right away so the command gets term'd immediately
	killCh := make(chan struct{})
	close(killCh)

	cmd := NewOsExec().Command("/bin/bash", tf.Name())
	rr := RunKillableCommand(cmd, killCh, 100*time.Millisecond, ioutil.Discard, 0)
	if rr.Error == nil {
		t.Fatal("unexpected nil error from RunKillableCommand")
	}
	// term results in !Exited and ExitCode -1, err is an ExitError
	var e *exitErrorAdapter
	if !errors.As(rr.Error, &e) {
		t.Fatal("error was not an os/exec.ExitError")
	}
	if rr.ProcessState.Exited() {
		t.Fatal("expected Exited false, got true")
	}
	if rr.ProcessState.ExitCode() != -1 {
		t.Fatalf("expected ExitCode -1, got: %d", rr.ProcessState.ExitCode())
	}
}

func TestRunKillableCommandKill(t *testing.T) {
	tf, err := setupTempScript(trapScript)
	if err != nil {
		t.Fatalf("failed setting up temp script file: %s", err)
	}
	defer os.Remove(tf.Name())

	killCh := make(chan struct{})
	go func() {
		// must wait until the script's "trap" command takes effect or the term will succeed in stopping it
		time.Sleep(1 * time.Second)
		close(killCh)
	}()

	cmd := NewOsExec().Command("/bin/bash", tf.Name())
	rr := RunKillableCommand(cmd, killCh, 100*time.Millisecond, ioutil.Discard, 0)
	if rr.Error == nil {
		t.Fatal("unexpected nil error from RunKillableCommand")
	}
	// kill results in !Exited and ExitCode -1, err is an ExitError
	var e *exitErrorAdapter
	if !errors.As(rr.Error, &e) {
		t.Fatal("error was not an os/exec.ExitError")
	}
	if rr.ProcessState.Exited() {
		t.Fatal("expected Exited false, got true")
	}
	if rr.ProcessState.ExitCode() != -1 {
		t.Fatalf("expected ExitCode -1, got: %d", rr.ProcessState.ExitCode())
	}
}

// TestValidatingExecer tests validating execer functionality
func TestValidatingExecer(t *testing.T) {
	expectedCmds := [][]string{{"testCmd1"}, {"testCmd2", "arg1"}, {"testCmd3"}}
	fakeActions := map[int]func(cmd Cmd) error{
		2: func(cmd Cmd) error {
			tCmd := cmd.(*ValidatingCmd)
			tCmd.GetStdout().Write([]byte("stdout!"))
			tCmd.GetStderr().Write([]byte("stderr!"))
			return nil
		},
	}
	ve := NewValidatingExecer(t, expectedCmds)
	defer ve.CheckAllValidated()
	ve.SetFakeActions(fakeActions)

	cmd := ve.Command("testCmd1")
	RunKillableCommand(cmd, nil, 100*time.Millisecond, ioutil.Discard, 0)

	cmd = ve.Command("testCmd2", "arg1")
	RunKillableCommand(cmd, nil, 100*time.Millisecond, ioutil.Discard, 0)

	var outBuf, errBuf bytes.Buffer
	cmd = ve.Command("testCmd3")
	cmd.SetStdout(&outBuf)
	cmd.SetStderr(&errBuf)

	assert.Nil(t, cmd.Run())
	assert.Equal(t, outBuf.String(), "stdout!")
	assert.Equal(t, errBuf.String(), "stderr!")
}

func setupTempScript(contents string) (*os.File, error) {
	tf, err := ioutil.TempFile("", "script")
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(tf.Name(), 0777); err != nil {
		os.Remove(tf.Name())
		return nil, err
	}
	if _, err := tf.Write([]byte(contents)); err != nil {
		os.Remove(tf.Name())
		return nil, err
	}
	return tf, nil
}

func TestCommandTimeout(t *testing.T) {
	cmd := NewOsExec().Command("sleep", "5")
	start := time.Now()
	rr := RunKillableCommand(cmd, nil, 1*time.Second, ioutil.Discard, 500*time.Millisecond)
	assert.True(t, time.Now().Sub(start) < time.Second)
	assert.True(t, errors.Is(rr.Error, TimeoutError))
}

func TestCommandNoTimeout(t *testing.T) {
	cmd := NewOsExec().Command("sleep", "2")
	start := time.Now()
	err := RunKillableCommand(cmd, nil, 1*time.Second, ioutil.Discard, 0)
	assert.True(t, time.Now().Sub(start) >= 2*time.Second)
	assert.Nil(t, err.Error)
}

func TestTruncateCmd(t *testing.T) {
	cmd := NewOsExec().Command("hello")
	assert.Equal(t, "hello", truncateCmd(cmd))

	cmd = NewOsExec().Command("hello", "world")
	assert.Equal(t, "hello world", truncateCmd(cmd))

	cmd = NewOsExec().Command("/foo/bar/xyz/hello", "world")
	assert.Equal(t, "hello world", truncateCmd(cmd))

	cmd = NewOsExec().Command("baz/foo/bar/xyz/hello", "world")
	assert.Equal(t, "hello world", truncateCmd(cmd))
}
