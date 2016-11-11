package runners

import (
	"fmt"
	"sync"

	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/snapshot"
)

const QueueFullMsg = "No resources available. Please try later."

// commandAndID is a command waiting to run in the queue and the ID we've assigned
type commandAndID struct {
	cmd *runner.Command
	id  runner.RunId
}

// NewQueueRunner creates a new runner with a queue of capacity
func NewQueueRunner(exec execer.Execer, filer snapshot.Filer, outputCreator runner.OutputCreator, capacity int) runner.Runner {
	statuses := NewStatuses()
	invoker := NewInvoker(exec, filer, outputCreator)
	controller := &QueueController{statuses: statuses, invoker: invoker, capacity: capacity}
	return NewControllerAndStatuserRunner(controller, statuses)
}

// TODO(dbentley): QueueController and SingleController are so similar...

// QueueController allows only one-at-a-time, but also allows queuing up to capacity cmds.
type QueueController struct {
	statuses *Statuses
	invoker  *Invoker
	capacity int

	runningID runner.RunId
	abortCh   chan struct{}
	queue     []commandAndID
	mu        sync.Mutex
}

// Run runs cmd, returning its initial status or an error
func (c *QueueController) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.queue) >= c.capacity {
		return runner.ProcessStatus{}, fmt.Errorf(QueueFullMsg)
	}
	st := c.statuses.NewRun()
	if c.runningID == runner.RunId("") {
		return c.start(cmd, st.RunId)
	} else {
		c.queue = append(c.queue, commandAndID{cmd, st.RunId})
		return st, nil
	}
}

// Abort aborts runId, returning the current (final) status of runId
func (c *QueueController) Abort(runId runner.RunId) (runner.ProcessStatus, error) {
	c.mu.Lock()

	if runId == c.runningID {
		if c.abortCh != nil {
			close(c.abortCh)
			c.abortCh = nil
		}
	} else {
		for i, cmdAndID := range c.queue {
			if runId == cmdAndID.id {
				c.queue = append(c.queue[:i], c.queue[i+1:]...)
				c.statuses.Update(runner.AbortStatus(runId))
			}
		}
	}

	// Unlock so the abort can call finish()
	c.mu.Unlock()
	return c.statuses.StatusQuerySingle(runner.RunDone(runId), runner.Wait())
}

// start starts a command, returning the current status
func (c *QueueController) start(cmd *runner.Command, id runner.RunId) (runner.ProcessStatus, error) {
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
		close(updateCh)
		c.finish(st)
	}()
	return c.statuses.Status(id)
}

// finish is a callback to be called when a process is finished with its final state
func (c *QueueController) finish(st runner.ProcessStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.runningID = ""
	c.abortCh = nil

	if len(c.queue) > 0 {
		cmdAndID := c.queue[0]
		c.queue = c.queue[1:]
		c.start(cmdAndID.cmd, cmdAndID.id)
	}

	c.statuses.Update(st)
}
