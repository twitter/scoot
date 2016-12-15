package local

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/snapshot"
)

const RunnerBusyMsg = "Runner is busy"

// NewSimpleRunner creates a runner that will run using the supplied helpers
func NewSimpleRunner(exec execer.Execer, filer snapshot.Filer, outputCreator runner.OutputCreator) runner.Runner {
	return &simpleRunner{
		exec:          exec,
		filer:         filer,
		outputCreator: outputCreator,
		runs:          make(map[runner.RunId]runner.ProcessStatus),
		availCh:       nil,
	}
}

// NewSimpleReportBackRunner is like NewSimpleRunner, but will also send to availCh when the runner is available
func NewSimpleReportBackRunner(exec execer.Execer, filer snapshot.Filer, outputCreator runner.OutputCreator, availCh chan struct{}) *simpleRunner {
	return &simpleRunner{
		exec:          exec,
		filer:         filer,
		outputCreator: outputCreator,
		runs:          make(map[runner.RunId]runner.ProcessStatus),
		availCh:       availCh,
	}
}

// simpleRunner runs one process at a time and stores results.
type simpleRunner struct {
	exec          execer.Execer
	filer         snapshot.Filer
	outputCreator runner.OutputCreator
	runs          map[runner.RunId]runner.ProcessStatus
	running       *runInstance
	nextRunId     int64
	availCh       chan struct{}
	mu            sync.Mutex
}

type runInstance struct {
	id     runner.RunId
	doneCh chan struct{}
}

func (r *simpleRunner) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	runId := runner.RunId(fmt.Sprintf("%d", r.nextRunId))
	r.nextRunId++

	if r.running != nil {
		return runner.ProcessStatus{}, fmt.Errorf(runner.RunnerBusyMsg)
	}

	// Redirect command output to the stdout/stderr Output vars. Outputs shall be closed in run().
	stdout, err := r.outputCreator.Create(fmt.Sprintf("%s-stdout", runId))
	if err != nil {
		r.updateStatus(runner.ErrorStatus(runId, fmt.Errorf("could not create stdout: %v", err)))
		return runner.ProcessStatus{}, fmt.Errorf(runner.LoggingErrMsg)
	}
	stderr, err := r.outputCreator.Create(fmt.Sprintf("%s-stderr", runId))
	if err != nil {
		stdout.Close()
		r.updateStatus(runner.ErrorStatus(runId, fmt.Errorf("could not create stderr: %v", err)))
		return runner.ProcessStatus{}, fmt.Errorf(runner.LoggingErrMsg)
	}

	// Assign a new run and set its status to Preparing.
	r.running = &runInstance{id: runId, doneCh: make(chan struct{})}
	status := runner.PreparingStatus(runId)
	status.StdoutRef = stdout.URI()
	status.StderrRef = stderr.URI()
	r.runs[runId] = status

	// Run in a new goroutine
	go r.run(cmd, runId, stdout, stderr, r.running.doneCh)
	if cmd.Timeout > 0 { // Timeout if applicable
		time.AfterFunc(cmd.Timeout, func() { r.updateStatus(runner.TimeoutStatus(runId)) })
	}
	// TODO(dbentley): we return PREPARING now to defend against long-checkout
	// But we could sleep short (50ms?), query status, and return that to capture the common, fast case
	return r.runs[runId], nil
}

func (r *simpleRunner) Status(runId runner.RunId) (runner.ProcessStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result, ok := r.runs[runId]
	if !ok {
		return runner.ProcessStatus{}, fmt.Errorf("could not find: %v", runId)
	}
	return result, nil
}

func (r *simpleRunner) StatusAll() ([]runner.ProcessStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	statuses := []runner.ProcessStatus{}
	for _, status := range r.runs {
		statuses = append(statuses, status)
	}
	return statuses, nil
}

func (r *simpleRunner) Abort(runId runner.RunId) (runner.ProcessStatus, error) {
	return r.updateStatus(runner.AbortStatus(runId))
}

func (r *simpleRunner) Close() error {
	if r.availCh != nil {
		close(r.availCh)
	}
	return nil
}

func (r *simpleRunner) Erase(runId runner.RunId) error {
	// Best effort is fine here.
	r.mu.Lock()
	defer r.mu.Unlock()
	if result, ok := r.runs[runId]; ok && result.State.IsDone() {
		delete(r.runs, runId)
	}
	return nil
}

func (r *simpleRunner) updateStatus(newStatus runner.ProcessStatus) (runner.ProcessStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	oldStatus, ok := r.runs[newStatus.RunId]
	if !ok {
		return runner.ProcessStatus{}, fmt.Errorf("cannot find run %v", newStatus.RunId)
	}

	if oldStatus.State.IsDone() {
		return oldStatus, nil
	}

	if newStatus.State.IsDone() {
		if newStatus.StdoutRef == "" {
			newStatus.StdoutRef = oldStatus.StdoutRef
		}
		if newStatus.StderrRef == "" {
			newStatus.StderrRef = oldStatus.StderrRef
		}

		// We are ending the running task.
		// depend on the invariant that there is at most 1 run with !state.IsDone(),
		// so if we're changing a Process from not Done to Done it must be running
		log.Printf("local.simpleRunner: run done. %+v", newStatus)
		close(r.running.doneCh)
		if r.availCh != nil {
			r.availCh <- struct{}{}
		}
		r.running = nil
	}

	r.runs[newStatus.RunId] = newStatus
	return newStatus, nil
}

// run cmd in the background, writing results to r as id, unless doneCh is closed. Closes stdout/stderr when finished.
func (r *simpleRunner) run(cmd *runner.Command, runId runner.RunId, stdout, stderr runner.Output, doneCh chan struct{}) {
	log.Printf("local.simpleRunner.run running: ID: %v, cmd: %+v", runId, cmd)
	defer stdout.Close()
	defer stderr.Close()

	checkout, err, checkoutDone := (snapshot.Checkout)(nil), (error)(nil), make(chan struct{})
	go func() {
		checkout, err = r.filer.Checkout(cmd.SnapshotId)
		close(checkoutDone)
	}()

	// Wait for checkout or cancel
	select {
	case <-doneCh:
		go func() {
			<-checkoutDone
			if checkout != nil {
				checkout.Release()
			}
		}()
		return
	case <-checkoutDone:
		if err != nil {
			r.updateStatus(runner.ErrorStatus(runId, fmt.Errorf("could not checkout: %v", err)))
			return
		}
	}
	defer checkout.Release()

	log.Printf("local.simpleRunner.run checkout: %v", checkout.Path())

	p, err := r.exec.Exec(execer.Command{
		Argv:   cmd.Argv,
		Dir:    checkout.Path(),
		Stdout: stdout,
		Stderr: stderr,
	})
	if err != nil {
		r.updateStatus(runner.ErrorStatus(runId, fmt.Errorf("could not exec: %v", err)))
		return
	}

	r.updateStatus(runner.RunningStatus(runId, stdout.URI(), stderr.URI()))

	processCh := make(chan execer.ProcessStatus, 1)
	go func() { processCh <- p.Wait() }()
	var st execer.ProcessStatus

	// Wait for process complete or cancel
	select {
	case <-doneCh:
		p.Abort()
		return
	case st = <-processCh:
	}

	switch st.State {
	case execer.COMPLETE:
		srcToDest := map[string]string{
			checkout.Path(): "",
			stdout.AsFile(): "STDOUT",
			stderr.AsFile(): "STDERR",
		}
		// TODO(jschiller): get consensus on design and either implement or delete.
		// if cmd.SnapshotPlan != nil {
		// 	for src, dest := range cmd.SnapshotPlan {
		// 		srcToDest[checkout.Path()+"/"+src] = dest // manually concat to preserve src *exactly* as provided.
		// 	}
		// }
		snapshotId, err := r.filer.IngestMap(srcToDest)
		if err != nil {
			r.updateStatus(runner.ErrorStatus(runId, fmt.Errorf("error ingesting results: %v", err)))
		} else {
			r.updateStatus(runner.CompleteStatus(runId, runner.SnapshotId(snapshotId), st.ExitCode))
		}
	case execer.FAILED:
		r.updateStatus(runner.ErrorStatus(runId, fmt.Errorf("error execing: %v", st.Error)))
	default:
		r.updateStatus(runner.ErrorStatus(runId, fmt.Errorf("unexpected exec state: %v", st.State)))
	}
}
