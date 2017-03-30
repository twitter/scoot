package runners

import (
	"fmt"
	"github.com/scootdev/scoot/common/log"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/runner/execer/execers"
	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/git/gitfiler"
)

// invoke.go: Invoker runs a Scoot command.

// NewInvoker creates an Invoker that will use the supplied helpers
func NewInvoker(exec execer.Execer, filer snapshot.Filer, output runner.OutputCreator, tmp *temp.TempDir) *Invoker {
	return &Invoker{exec: exec, filer: filer, output: output, tmp: tmp}
}

// TODO(dbentley): test this separately from the end-to-end runner tests

// Invoker Runs a Scoot Command by performing the Scoot setup and gathering.
// (E.g., checking out a Snapshot, or saving the Output once it's done)
// Unlike a full Runner, it has no idea of what else is running or has run.
type Invoker struct {
	exec   execer.Execer
	filer  snapshot.Filer
	output runner.OutputCreator
	tmp    *temp.TempDir
}

// Run runs cmd
// Run will send updates as the process is running to updateCh.
// The RunStatus'es that come out of updateCh will have an empty RunID
// Run will enforce cmd's Timeout, and will abort cmd if abortCh is signaled.
// updateCh will not close until the run is finished running.
func (inv *Invoker) Run(cmd *runner.Command, id runner.RunID) (abortCh chan<- struct{}, updateCh <-chan runner.RunStatus) {
	abortChFull := make(chan struct{})
	updateChFull := make(chan runner.RunStatus)
	go inv.run(cmd, id, abortChFull, updateChFull)
	return abortChFull, updateChFull
}

// Run runs cmd as run id returning the final ProcessStatus
// Run will send updates the process is running to updateCh.
// Run will enforce cmd's Timeout, and will abort cmd if abortCh is signaled.
// Run will not return until the process is not running.
// NOTE: kind of gnarly since our filer implementation (gitdb) currently mutates the same worktree on every op.
func (inv *Invoker) run(cmd *runner.Command, id runner.RunID, abortCh chan struct{}, updateCh chan runner.RunStatus) (r runner.RunStatus) {
	log.Info("run. id: %v, cmd: %+v", id, cmd)
	defer func() {
		updateCh <- r
		close(updateCh)
	}()

	var co snapshot.Checkout
	checkoutCh := make(chan error)
	go func() {
		if cmd.SnapshotID == "" {
			//TODO: we don't want this logic to live here, these decisions should be made at a higher level.
			if len(cmd.Argv) > 0 && cmd.Argv[0] != execers.UseSimExecerArg {
				log.Info("RunID:%s has no snapshotID! Using a nop-checkout initialized with tmpDir.\n", id)
			}
			if tmp, err := inv.tmp.TempDir("invoke_nop_checkout"); err != nil {
				checkoutCh <- err
			} else {
				co = gitfiler.MakeUnmanagedCheckout(string(id), tmp.Dir)
				checkoutCh <- nil
			}
		} else {
			//NOTE: given the current gitdb impl, this checkout will block until the previous checkout is released.
			log.Info("RunID:%s checking out snapshotID:%s", id, cmd.SnapshotID)
			var err error
			co, err = inv.filer.Checkout(cmd.SnapshotID)
			checkoutCh <- err
		}
	}()

	select {
	case <-abortCh:
		go func() {
			if err := <-checkoutCh; err != nil {
				// If there was an error there should be no lingering gitdb locks, so return.
				return
			}
			// If there was no error then we need to release this checkout.
			co.Release()
		}()
		return runner.AbortStatus(id)
	case err := <-checkoutCh:
		if err != nil {
			return runner.ErrorStatus(id, err)
		}
		// Checkout is ok, continue with run and when finished release checkout.
		defer co.Release()
	}
	log.Info("checkout done. id: %v, cmd: %+v, checkout: %v", id, cmd, co.Path())

	stdout, err := inv.output.Create(fmt.Sprintf("%s-stdout", id))
	if err != nil {
		return runner.ErrorStatus(id, fmt.Errorf("could not create stdout: %v", err))
	}
	defer stdout.Close()

	stderr, err := inv.output.Create(fmt.Sprintf("%s-stderr", id))
	if err != nil {
		return runner.ErrorStatus(id, fmt.Errorf("could not create stderr: %v", err))
	}
	defer stderr.Close()

	marker := "###########################################\n###########################################\n"
	format := "%s\n\nDate: %v\nSelf: %s\tCmd:\n%v\n\n%s\n\n\nSCOOT_CMD_LOG\n"
	stdout.Write([]byte(fmt.Sprintf(format, marker, time.Now(), stdout.URI(), cmd, marker)))
	stderr.Write([]byte(fmt.Sprintf(format, marker, time.Now(), stderr.URI(), cmd, marker)))
	log.Info("RunID: %s, stdout: %s, stderr: %s\n", id, stdout.AsFile(), stderr.AsFile())

	p, err := inv.exec.Exec(execer.Command{
		Argv:   cmd.Argv,
		Dir:    co.Path(),
		Stdout: stdout,
		Stderr: stderr,
	})
	if err != nil {
		return runner.ErrorStatus(id, fmt.Errorf("could not exec: %v", err))
	}

	var timeoutCh <-chan time.Time
	if cmd.Timeout > 0 { // Timeout if applicable
		timeout := time.NewTicker(cmd.Timeout)
		timeoutCh = timeout.C
		defer timeout.Stop()
	}

	updateCh <- runner.RunningStatus(id, stdout.URI(), stderr.URI())

	processCh := make(chan execer.ProcessStatus, 1)
	go func() { processCh <- p.Wait() }()
	var st execer.ProcessStatus

	// Wait for process to complete (or cancel if we're told to)
	select {
	case <-abortCh:
		p.Abort()
		return runner.AbortStatus(id)
	case <-timeoutCh:
		p.Abort()
		return runner.TimeoutStatus(id)
	case st = <-processCh:
	}

	log.Info("run done. id: %v, status: %+v, cmd: %+v, checkout: %v", id, st, cmd, co.Path())

	switch st.State {
	case execer.COMPLETE:
		tmp, err := inv.tmp.TempDir("invoke")
		if err != nil {
			return runner.ErrorStatus(id, fmt.Errorf("error staging ingestion dir: %v", err))
		}
		defer os.RemoveAll(tmp.Dir)
		outPath := stdout.AsFile()
		errPath := stderr.AsFile()
		stdoutName := "STDOUT"
		stderrName := "STDERR"
		if writer, err := os.Create(filepath.Join(tmp.Dir, stdoutName)); err != nil {
			return runner.ErrorStatus(id, fmt.Errorf("error staging ingestion for stdout: %v", err))
		} else if reader, err := os.Open(outPath); err != nil {
			return runner.ErrorStatus(id, fmt.Errorf("error staging ingestion for stdout: %v", err))
		} else if _, err := io.Copy(writer, reader); err != nil {
			return runner.ErrorStatus(id, fmt.Errorf("error staging ingestion for stdout: %v", err))
		}
		if writer, err := os.Create(filepath.Join(tmp.Dir, stderrName)); err != nil {
			return runner.ErrorStatus(id, fmt.Errorf("error staging ingestion for stderr: %v", err))
		} else if reader, err := os.Open(errPath); err != nil {
			return runner.ErrorStatus(id, fmt.Errorf("error staging ingestion for stderr: %v", err))
		} else if _, err := io.Copy(writer, reader); err != nil {
			return runner.ErrorStatus(id, fmt.Errorf("error staging ingestion for stderr: %v", err))
		}
		snapshotID, err := inv.filer.Ingest(tmp.Dir)
		if err != nil {
			return runner.ErrorStatus(id, fmt.Errorf("error ingesting results: %v", err))
		}
		//TODO: stdout/stderr should configurably point to a bundlestore server addr.
		//Note: only modifying stdout/stderr refs when we're actively working with snapshotID.
		status := runner.CompleteStatus(id, snapshotID, st.ExitCode)
		if cmd.SnapshotID != "" {
			status.StdoutRef = snapshotID + "/" + stdoutName
			status.StderrRef = snapshotID + "/" + stderrName
		}
		return status
	case execer.FAILED:
		return runner.ErrorStatus(id, fmt.Errorf("error execing: %v", st.Error))
	default:
		return runner.ErrorStatus(id, fmt.Errorf("unexpected exec state: %v", st.State))
	}
}
