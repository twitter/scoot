package local

import (
	"fmt"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"log"
	"sync"
)

func NewSimpleRunner(exec execer.Execer) runner.Runner {
	r := &simpleRunner{}
	r.exec = exec
	r.runs = make(map[runner.RunId]runner.ProcessStatus)
	r.running = nil
	return r
}

type simpleRunner struct {
	exec      execer.Execer
	runs      map[runner.RunId]runner.ProcessStatus
	running   execer.Process
	nextRunId int64
	mu        sync.Mutex
}

func (r *simpleRunner) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	runId := runner.RunId(fmt.Sprintf("%d", r.nextRunId))
	r.nextRunId++

	if r.running != nil {
		r.runs[runId] = runner.ErrorStatus(runId, fmt.Errorf("Runner is busy"))
	}

	var c execer.Command
	c.Argv = cmd.Argv
	p, err := r.exec.Exec(c)
	if err != nil {
		r.runs[runId] = runner.ErrorStatus(runId, fmt.Errorf("could not exec: %v", err))
		return r.runs[runId], err
	}
	r.runs[runId] = runner.RunningStatus(runId)
	r.running = p

	go babysit(p, runId, r)

	log.Println("simpleRunner.Run: returning")

	return r.runs[runId], nil
}

func (r *simpleRunner) Status(run runner.RunId) (runner.ProcessStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result, ok := r.runs[run]
	if !ok {
		return result, fmt.Errorf("unknown runId")
	}
	return result, nil
}

func (r *simpleRunner) markFinished(p execer.Process, runId runner.RunId, status execer.ProcessStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	log.Println("simpleRunner.markFinished", runId, status)
	r.running = nil
	switch status.State {
	case execer.COMPLETE:
		log.Println("simpleRunner.markFinished complete")
		r.runs[runId] = runner.CompleteStatus(runId, status.StdoutURI, status.StderrURI, status.ExitCode)
	case execer.FAILED:
		log.Println("simpleRunner.markFinished failed")
		r.runs[runId] = runner.ErrorStatus(runId, fmt.Errorf("error execing: %v", status.Error))
	default:
		log.Println("simpleRunner.markFinished error")
		r.runs[runId] = runner.ErrorStatus(runId, fmt.Errorf("unexpected exec state: %v", status.State))
	}
}

func babysit(p execer.Process, runId runner.RunId, r *simpleRunner) {
	// TODO(dbentley): here is where we enforce timeout (calling p.Abort() if we go too long)
	log.Println("simple.go:babysit about to wait")

	status := p.Wait()
	log.Println("simple.go:babysit process done")
	r.markFinished(p, runId, status)
	log.Println("simple.go:babysit marked finished")
}
