package local

import (
	"fmt"
	"sync"
	"time"

	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/snapshots"
)

func NewSimpleRunner(exec execer.Execer, checkouter snapshots.Checkouter) runner.Runner {
	return &simpleRunner{
		exec:       exec,
		checkouter: checkouter,
		runs:       make(map[runner.RunId]runner.ProcessStatus),
	}
}

// simpleRunner runs one process at a time and stores results.
type simpleRunner struct {
	exec       execer.Execer
	checkouter snapshots.Checkouter
	runs       map[runner.RunId]runner.ProcessStatus
	running    *inflight
	nextRunId  int64
	mu         sync.Mutex
}

type inflight struct {
	runId    runner.RunId
	cancelCh chan struct{}
}

func (r *simpleRunner) Run(cmd *runner.Command) runner.ProcessStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	runId := runner.RunId(fmt.Sprintf("%d", r.nextRunId))
	r.nextRunId++

	if r.running != nil {
		r.runs[runId] = runner.BadRequestStatus(runId, fmt.Errorf("Runner is busy"))
		return r.runs[runId]
	}

	r.running = &inflight{runId: runId, cancelCh: make(chan struct{})}
	r.runs[runId] = runner.PreparingStatus(runId)

	// Run in a new goroutine
	go r.run(cmd, runId, r.running.cancelCh)
	if cmd.Timeout > 0 { // Timeout if applicable
		time.AfterFunc(cmd.Timeout, func() { r.updateStatus(runner.TimeoutStatus(runId)) })
	}
	// TODO(dbentley): we return PREPARING now to defend against long-checkout
	// But we could sleep short (50ms?), query status, and return that to capture the common, fast case
	return r.runs[runId]
}

func makeRunnerStatus(st execer.ProcessStatus, runId runner.RunId) runner.ProcessStatus {
	if st.State == execer.COMPLETE {
		return runner.CompleteStatus(runId, st.StdoutURI, st.StderrURI, st.ExitCode)
	} else if st.State == execer.FAILED {
		return runner.ErrorStatus(runId, fmt.Errorf("error execing: %v", st.Error))
	}
	return runner.ErrorStatus(runId, fmt.Errorf("unexpected exec state: %v", st.State))
}

func (r *simpleRunner) Status(run runner.RunId) runner.ProcessStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	result, ok := r.runs[run]
	if !ok {
		return runner.BadRequestStatus(run, fmt.Errorf("could not find: %v", run))
	}
	return result
}

func (r *simpleRunner) StatusAll() []runner.ProcessStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	statuses := []runner.ProcessStatus{}
	for _, status := range r.runs {
		statuses = append(statuses, status)
	}
	return statuses
}

func (r *simpleRunner) Abort(run runner.RunId) runner.ProcessStatus {
	return r.updateStatus(runner.AbortStatus(run))
}

func (r *simpleRunner) Erase(run runner.RunId) {
	// Best effort is fine here.
	r.mu.Lock()
	defer r.mu.Unlock()
	if result, ok := r.runs[run]; ok && result.State.IsDone() {
		delete(r.runs, run)
	}
}

func (r *simpleRunner) updateStatus(new runner.ProcessStatus) runner.ProcessStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	old, ok := r.runs[new.RunId]
	if !ok {
		return runner.BadRequestStatus(new.RunId, fmt.Errorf("cannot find run %v", new.RunId))
	}
	if old.State.IsDone() {
		return old
	}
	r.runs[new.RunId] = new
	if new.State.IsDone() && r.running.runId == new.RunId {
		close(r.running.cancelCh)
		r.running = nil
	}
	return new
}

// run cmd in the background, writing results to r as id, unless cancelCh is closed
func (r *simpleRunner) run(cmd *runner.Command, runId runner.RunId, cancelCh chan struct{}) {
	checkout, err, checkoutDone := (snapshots.Checkout)(nil), (error)(nil), make(chan struct{}, 1)
	go func() {
		checkout, err = r.checkouter.Checkout(cmd.SnapshotId)
		close(checkoutDone)
	}()

	// Wait for checkout or cancel
	select {
	case <-cancelCh:
		return
	case <-checkoutDone:
	}
	if err != nil {
		r.updateStatus(runner.FailedStatus(runId, fmt.Errorf("could not checkout: %v", err)))
		return
	}
	defer checkout.Release()

	p, err := r.exec.Exec(execer.Command{
		Argv: cmd.Argv,
		Dir:  checkout.Path(),
	})
	if err != nil {
		r.updateStatus(runner.FailedStatus(runId, fmt.Errorf("could not exec: %v", err)))
		return
	}

	r.updateStatus(runner.RunningStatus(runId))

	processCh := make(chan execer.ProcessStatus, 1)
	go func() { processCh <- p.Wait() }()

	// Wait for process complete or cancel
	select {
	case <-cancelCh:
		p.Abort()
		return
	case st := <-processCh:
		r.updateStatus(makeRunnerStatus(st, runId))
	}
}
