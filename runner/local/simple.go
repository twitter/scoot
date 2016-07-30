package local

import (
	"fmt"
	"sync"
	"time"

	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
)

func NewSimpleRunner(exec execer.Execer) runner.Runner {
	r := &simpleRunner{}
	r.exec = exec
	r.runs = make(map[runner.RunId]runner.ProcessStatus)
	r.running = make(map[runner.RunId]execer.Process)
	return r
}

// simpleRunner runs N=maxRunning processes at a time and stores results.
type simpleRunner struct {
	exec      execer.Execer
	runs      map[runner.RunId]runner.ProcessStatus
	running   map[runner.RunId]execer.Process
	nextRunId int64
	mu        sync.Mutex
}

func (r *simpleRunner) Run(cmd *runner.Command) runner.ProcessStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	runId := runner.RunId(fmt.Sprintf("%d", r.nextRunId))
	r.nextRunId++

	if len(r.running) == 1 {
		r.runs[runId] = runner.BadRequestStatus(runId, fmt.Errorf("Runner is busy"))
		return r.runs[runId]
	}

	var c execer.Command
	c.Argv = cmd.Argv
	p, err := r.exec.Exec(c)
	if err != nil {
		r.runs[runId] = runner.BadRequestStatus(runId, fmt.Errorf("could not exec: %v", err))
		return r.runs[runId]
	}
	r.runs[runId] = runner.RunningStatus(runId)
	r.running[runId] = p

	go babysit(p, runId, r, cmd.Timeout)

	return r.runs[runId]
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
	return r.abortImpl(run, runner.ABORTED)
}

func (r *simpleRunner) Erase(run runner.RunId) {
	// Best effort is fine here.
	r.mu.Lock()
	defer r.mu.Unlock()
	if result, ok := r.runs[run]; ok && result.State.IsDone() {
		delete(r.runs, run)
	}
}

func (r *simpleRunner) abortImpl(run runner.RunId, state runner.ProcessState) runner.ProcessStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	result, ok := r.running[run]
	if !ok {
		return runner.BadRequestStatus(run, fmt.Errorf("could not find abortable run: %v", run))
	}
	result.Abort()
	delete(r.running, run)
	r.runs[run] = runner.AbortStatus(run)
	return r.runs[run]
}

func babysit(p execer.Process, run runner.RunId, r *simpleRunner, timeout time.Duration) {
	done := make(chan interface{})
	if timeout > 0 {
		go func() {
			select {
			case <-time.NewTimer(timeout).C:
				r.abortImpl(run, runner.TIMEDOUT)
			case <-done:
				return
			}
		}()
	}

	status := p.Wait()
	if timeout > 0 {
		done <- nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.running, run)

	// Check if we already marked this run as done (i.e. in Abort())
	if r.runs[run].State.IsDone() {
		return
	}

	switch status.State {
	case execer.COMPLETE:
		r.runs[run] = runner.CompleteStatus(run, status.StdoutURI, status.StderrURI, status.ExitCode)
	case execer.FAILED:
		r.runs[run] = runner.ErrorStatus(run, fmt.Errorf("error execing: %v", status.Error))
	default:
		r.runs[run] = runner.ErrorStatus(run, fmt.Errorf("unexpected exec state: %v", status.State))
	}
}
