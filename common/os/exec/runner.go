package exec

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	CmdSeparator = "--------------------------------------------------------------"
)

var TimeoutError = errors.New("command timeout")

// RunResult encapsulates a return from RunKillableCommand. It is largely a summary of fields
// available from os/exec.Cmd structs, plus the full contents of stdout and stderr for analysis.
type RunResult struct {
	// ProcessState contains information about an exited process.
	// A command that fails to start or run may have a nil ProcessState.
	ProcessState *os.ProcessState

	// Stdout and Stderr contain the contents of a completed process's outputs.
	Stdout []byte
	Stderr []byte

	// Error contains any error from exec.Cmd Start() or Wait().
	Error error
}

func (rr RunResult) String() string {
	return fmt.Sprintf("Error:%s, Stdout:%s, Stderr:%s", rr.Error, rr.Stdout, rr.Stderr)
}

func truncateCmd(cmd Cmd) string {
	args := cmd.Args()
	if len(args) > 0 {
		args[0] = filepath.Base(args[0])
	}
	return strings.Join(args, " ")
}

// RunKillableCommand execs the given Cmd and returns the resulting ProcessState and stdout/stderr contents.
// All output content is returned, but combined content can also be streamed as the Cmd is executed to streamLog.
// Accepts a killCh that, if received on, will SIGTERM up until the specified killTimeout after which the
// process is killed. Returns the error value from Cmd.Start() or Cmd.Wait().
// Also accepts timeout param.  If timeout param > 0 the execution will be killed if it does not complete
// within timeout.
func RunKillableCommand(
	cmd Cmd,
	killCh <-chan struct{},
	killTimeout time.Duration,
	streamLog io.Writer,
	timeout time.Duration,
) RunResult {
	rr := RunResult{nil, nil, nil, nil}

	// send stdout/stderr to both streamLog and outBuf/errBuf
	var outBuf, errBuf bytes.Buffer
	syncLog := &syncWriter{w: streamLog}
	cmd.SetStdout(io.MultiWriter(&outBuf, syncLog))
	cmd.SetStderr(io.MultiWriter(&errBuf, syncLog))

	doneCh := make(chan struct{})

	log.Infof("Running Command: %s", cmd.String())
	syncLog.Write([]byte(fmt.Sprintf("\n%s\nRunning Command: %s\n", CmdSeparator, truncateCmd(cmd))))
	cmdErr := cmd.Start()
	if cmdErr != nil {
		rr.Error = cmdErr
		rr.Stdout = outBuf.Bytes()
		rr.Stderr = errBuf.Bytes()
		return rr
	}

	go func() {
		cmdErr = cmd.Wait()
		close(doneCh)
	}()

	// if timeout > 0, set up timeout signalling
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timeoutCh = time.After(timeout)
	} else {
		timeoutCh = make(chan time.Time)
	}

	// wait for command completion, timeout or kill
	select {
	case <-doneCh:
		syncLog.Write([]byte(fmt.Sprintf("\nExited - ExitCode: %d\n%s\n", cmd.ProcessState().ExitCode(), CmdSeparator)))
	case <-timeoutCh:
		log.Infof("command timed out %v. Killing command", timeout)
		termThenKill(cmd.Process(), killTimeout, doneCh)
		// must still wait for cmd.Wait()
		<-doneCh
		syncLog.Write([]byte(fmt.Sprintf("\nTimeout after %v\n%s\n", timeout, CmdSeparator)))
		cmdErr = TimeoutError
	case <-killCh:
		log.Info("Received kill request for command")
		termThenKill(cmd.Process(), killTimeout, doneCh)
		// must still wait for cmd.Wait()
		<-doneCh
		syncLog.Write([]byte(fmt.Sprintf("\nTerminated by external request\n%s\n", CmdSeparator)))
	}

	rr.ProcessState = cmd.ProcessState()
	rr.Stdout = outBuf.Bytes()
	rr.Stderr = errBuf.Bytes()
	rr.Error = cmdErr
	return rr
}

// termThenKill will SIGTERM a process, then Kill it if it hasn't exited after duration d.
// waitDoneCh must be closed by the caller when the process exits (to avoid double Wait()ing)
func termThenKill(p *os.Process, d time.Duration, waitDoneCh <-chan struct{}) error {
	if p == nil {
		return nil
	}
	log.Info("Sending SIGTERM to command")
	err := p.Signal(syscall.SIGTERM)
	if err != nil {
		log.Errorf("Failed to send SIGTERM to process: %s", err)
		return err
	}

	select {
	case <-waitDoneCh:
	case <-time.After(d):
		log.Info("Command hasn't exited, using Kill()")
		err = p.Kill()
		if err != nil {
			log.Errorf("Failed to Kill() process: %s", err)
			return err
		}
	}
	return nil
}

// syncWriter is an io.Writer wrapper around another io.Writer that supports safe concurrent Writes.
// RunKillableCommand needs to use this to safely write both stdout and stderr to streamLog.
type syncWriter struct {
	w  io.Writer
	mu sync.Mutex
}

func (b *syncWriter) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.w.Write(p)
}
