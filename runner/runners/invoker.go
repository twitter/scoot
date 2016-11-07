package runners

import (
	"fmt"
	"log"
	"time"

	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/snapshot"
)

// NewInvoker creates a NewExecer that will use the supplied helpers
func NewInvoker(exec execer.Execer, filer snapshot.Filer, outputCreator runner.OutputCreator) *Invoker {
	return &Invoker{exec, filer, outputCreator}
}

// Invoker Runs a Scoot Command by performing the Scoot setup and gathering.
// (E.g., checking out a Snapshot, or saving the Output once it's done)
// Unlike a full Runner, it has no idea of what else is running or has run.
type Invoker struct {
	execer        execer.Execer
	filer         snapshot.Filer
	outputCreator runner.OutputCreator
}

type checkoutAndError struct {
	checkout snapshot.Checkout
	err      error
}

// Run runs cmd as run id returning the final ProcessStatus
// Run will send updates the process is running to updateCh.
// Run will enforce cmd's Timeout, and will abort cmd if abortCh is signaled.
// Run will not return until the process is not running.
func (in *Invoker) Run(cmd *runner.Command, id runner.RunId, abortCh chan struct{}, updateCh chan runner.ProcessStatus) runner.ProcessStatus {
	log.Printf("runner/local/invoker.go: Run id %v with cmd %+v", id, cmd)

	var timeoutCh <-chan time.Time
	if cmd.Timeout > 0 { // Timeout if applicable
		timeout := time.NewTicker(cmd.Timeout)
		timeoutCh = timeout.C
		defer timeout.Stop()
	}

	checkoutCh := make(chan checkoutAndError)
	var checkout snapshot.Checkout
	var err error
	go func() {
		checkout, err = in.filer.Checkout(cmd.SnapshotId)
		checkoutCh <- checkoutAndError{checkout, err}
	}()

	drainCheckout := func() {
		checkoutAndErr := <-checkoutCh
		if checkoutAndErr.err != nil {
			return
		}
		checkoutAndErr.checkout.Release()
	}

	select {
	case <-abortCh:
		go drainCheckout()
		return runner.AbortStatus(id)
	case <-timeoutCh:
		go drainCheckout()
		return runner.TimeoutStatus(id)
	case checkoutAndErr := <-checkoutCh:
		checkout, err = checkoutAndErr.checkout, checkoutAndErr.err
	}

	defer checkout.Release()

	log.Printf("runner.local.invoker.Run cmd: %+v checkout: %v", cmd, checkout.Path())

	stdout, err := in.outputCreator.Create(fmt.Sprintf("%s-stdout", id))
	if err != nil {
		return runner.ErrorStatus(id, fmt.Errorf("could not create stdout: %v", err))
	}
	defer stdout.Close()
	stderr, err := in.outputCreator.Create(fmt.Sprintf("%s-stderr", id))
	if err != nil {
		return runner.ErrorStatus(id, fmt.Errorf("could not create stderr: %v", err))
	}
	defer stderr.Close()

	p, err := in.execer.Exec(execer.Command{
		Argv:   cmd.Argv,
		Dir:    checkout.Path(),
		Stdout: stdout,
		Stderr: stderr,
	})
	if err != nil {
		return runner.ErrorStatus(id, fmt.Errorf("could not exec: %v", err))
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
		snapshotID, err := in.filer.IngestMap(srcToDest)
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
