package runners

import (
	"fmt"
	"log"
	"time"

	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/snapshot"
)

// invoke.go: Invoker runs a Scoot command.

// NewInvoker creates an Invoker that will use the supplied helpers
func NewInvoker(exec execer.Execer, filer snapshot.Filer, output runner.OutputCreator) *Invoker {
	return &Invoker{exec: exec, filer: filer, output: output}
}

// TODO(dbentley): test this separately from the end-to-end runner tests

// Invoker Runs a Scoot Command by performing the Scoot setup and gathering.
// (E.g., checking out a Snapshot, or saving the Output once it's done)
// Unlike a full Runner, it has no idea of what else is running or has run.
type Invoker struct {
	exec   execer.Execer
	filer  snapshot.Filer
	output runner.OutputCreator
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

type checkoutAndError struct {
	checkout snapshot.Checkout
	err      error
}

// Run runs cmd as run id returning the final ProcessStatus
// Run will send updates the process is running to updateCh.
// Run will enforce cmd's Timeout, and will abort cmd if abortCh is signaled.
// Run will not return until the process is not running.
func (inv *Invoker) run(cmd *runner.Command, id runner.RunID, abortCh chan struct{}, updateCh chan runner.RunStatus) (r runner.RunStatus) {
	log.Printf("runner/runners/invoke.go: run. id %v cmd %+v", id, cmd)
	defer func() {
		updateCh <- r
		close(updateCh)
	}()

	checkoutCh := make(chan checkoutAndError)
	var checkout snapshot.Checkout
	var err error
	go func() {
		checkout, err = inv.filer.Checkout(cmd.SnapshotID)
		checkoutCh <- checkoutAndError{checkout, err}
	}()

	select {
	case <-abortCh:
		go func() {
			checkoutAndErr := <-checkoutCh
			if checkoutAndErr.err != nil {
				return
			}
			checkoutAndErr.checkout.Release()
		}()
		return runner.AbortStatus(id)
	case checkoutAndErr := <-checkoutCh:
		if checkout, err = checkoutAndErr.checkout, checkoutAndErr.err; err != nil {
			return runner.ErrorStatus(id, err)
		}
	}

	defer checkout.Release()

	log.Printf("runner/runners/invoke.go: checkout done. id %v cmd: %+v checkout: %v", id, cmd, checkout.Path())

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

	p, err := inv.exec.Exec(execer.Command{
		Argv:   cmd.Argv,
		Dir:    checkout.Path(),
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
	// Periodically check to make sure memory constraints are respected.
	//TODO: may want to make this configurable.
	memTicker := &time.Ticker{}
	if cmd.MemoryCap != 0 {
		memTicker = time.NewTicker(100 * time.Millisecond)
		defer memTicker.Stop()
	}
Loop:
	for {
		select {
		case <-memTicker.C:
			usage, _ := p.MemUsage()
			if usage >= cmd.MemoryCap {
				p.Abort()
				err := fmt.Errorf("Cmd exceeded MemoryCap: %d > %d", usage, cmd.MemoryCap)
				return runner.ErrorStatus(id, err)
			}
		case <-abortCh:
			p.Abort()
			return runner.AbortStatus(id)
		case <-timeoutCh:
			p.Abort()
			return runner.TimeoutStatus(id)
		case st = <-processCh:
			break Loop
		}
	}

	log.Printf("runner/runners/invoke.go: run done. id %v status %+v cmd: %+v checkout: %v", id, st, cmd, checkout.Path())

	switch st.State {
	case execer.COMPLETE:
		srcToDest := map[string]string{
			stdout.AsFile(): "STDOUT",
			stderr.AsFile(): "STDERR",
		}
		// TODO(jschiller): get consensus on design and either implement or delete.
		// if cmd.SnapshotPlan != nil {
		// 	for src, dest := range cmd.SnapshotPlan {
		// 		srcToDest[checkout.Path()+"/"+src] = dest // manually concat to preserve src *exactly* as provided.
		// 	}
		// }
		snapshotID, err := inv.filer.IngestMap(srcToDest)
		if err != nil {
			return runner.ErrorStatus(id, fmt.Errorf("error ingesting results: %v", err))
		}
		return runner.CompleteStatus(id, snapshotID, st.ExitCode)
	case execer.FAILED:
		return runner.ErrorStatus(id, fmt.Errorf("error execing: %v", st.Error))
	default:
		return runner.ErrorStatus(id, fmt.Errorf("unexpected exec state: %v", st.State))
	}
}
