package runners

import (
	"fmt"
	"sync"

	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/snapshot"
)

const RunnerBusyMsg = "Runner is busy"

// NewSingleRunner creates a new Runner that will run a single Process at a time
func NewSingleRunner(exec execer.Execer, filer snapshot.Filer, outputCreator runner.OutputCreator) runner.Runner {
	statuses := NewStatuses()
	invoker := NewInvoker(exec, filer, outputCreator)
	controller := &SingleController{statuses: statuses, invoker: invoker}
	return NewControllerAndStatuserRunner(controller, statuses)
}

// SingleController will run a single Process at a time, with no queue
type SingleController struct {
	statuses *Statuses
	invoker  *Invoker

	runningID runner.RunId
	abortCh   chan struct{}
	mu        sync.Mutex
}

func (c *SingleController) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.runningID != "" {
		return runner.ProcessStatus{}, fmt.Errorf(RunnerBusyMsg)
	}

	st := c.statuses.NewRun()

	return c.start(cmd, st.RunId)
}

func (c *SingleController) Abort(runId runner.RunId) (runner.ProcessStatus, error) {
	c.mu.Lock()
	if runId != c.runningID {
		c.mu.Unlock()
		return c.statuses.StatusQuerySingle(runner.RunDone(runId), runner.Current())
	}

	if c.abortCh != nil {
		close(c.abortCh)
		c.abortCh = nil
	}
	// Unlock so the abort can call finish()
	c.mu.Unlock()

	return c.statuses.StatusQuerySingle(runner.RunDone(runId), runner.Wait())
}

func (c *SingleController) start(cmd *runner.Command, id runner.RunId) (runner.ProcessStatus, error) {
	c.runningID = id
	c.abortCh = make(chan struct{})
	updateCh := make(chan runner.ProcessStatus)
	go func() {
		for st := range updateCh {
			c.statuses.Update(st)
		}
	}()

	go func() {
		st := c.invoker.Run(cmd, c.runningID, c.abortCh, updateCh)
		c.finish(st)
	}()
	return c.statuses.Status(id)
}

func (c *SingleController) finish(st runner.ProcessStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.runningID = ""
	c.abortCh = nil
	c.statuses.Update(st)
}
