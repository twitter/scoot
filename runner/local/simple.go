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

func NewSimpleRunner(exec execer.Execer, checkouter snapshot.Checkouter, outputCreator runner.OutputCreator) runner.Runner {
	return &simpleRunner{}
}

type SimpleRunner struct {
	statuses *Statuses
	invoker  *Invoker

	runningID runner.RunId
	abortCh   chan struct{}
	mu        sync.Mutex
}

func (c *SimpleRunner) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.runningID != "" {
		return runner.ProcessStatus{}, fmt.Errorf(RunnerBusyMsg)
	}

	st := c.statuses.NewRun()

	return c.start(cmd, st.RunId)
}

func (c *SimpleRunner) Abort(runId runner.RunId) (runner.ProcessStatus, error) {
	c.mu.Lock()
	if runId != c.runningID {
		c.mu.Unlock()
		return c.Statuses.Status(runId)
	}

	if c.abortCh != nil {
		close(c.abortCh)
		c.abortCh = nil
	}
	// Unlock so the abort can call finish()
	c.mu.Unlock()

	return c.Statuses.QuerySingle(runner.RunDone(runId), runner.Wait())
}

func (c *SimpleRunner) start(cmd *runner.Command, id runner.RunId) (runner.ProcessStatus, error) {
	c.runningID = id
	c.abortCh = make(chan struct{})
	updateCh := make(chan runner.ProcessStatus)
	go func() {
		for st := range c.updateCh {
			c.statuses.Update(st)
		}
	}()

	go func() {
		st := c.invoker.Run(cmd, c.runningID, updateCh, c.abortCh)
		c.finish(st)
	}()
	return st, nil
}

func (c *SimpleRunner) finish(st runner.ProcessStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.runningID = ""
	c.abortCh = nil
	c.statuses.Update(final)
}
