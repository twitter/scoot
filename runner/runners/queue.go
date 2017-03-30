package runners

import (
	"fmt"
	"sync"

	"github.com/scootdev/scoot/common/log"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/snapshot"
)

const QueueFullMsg = "No resources available. Please try later."

type cmdAndID struct {
	cmd *runner.Command
	id  runner.RunID
}

// NewQueueRunner creates a new Service that uses a Queue
func NewQueueRunner(
	exec execer.Execer, filer snapshot.Filer, output runner.OutputCreator, tmp *temp.TempDir, capacity int,
) runner.Service {
	statusManager := NewStatusManager()
	inv := NewInvoker(exec, filer, output, tmp)
	controller := &QueueController{statusManager: statusManager, inv: inv, capacity: capacity}
	runner := &Service{controller, statusManager, statusManager}
	return runner
}

func NewSingleRunner(exec execer.Execer, filer snapshot.Filer, output runner.OutputCreator, tmp *temp.TempDir) runner.Service {
	return NewQueueRunner(exec, filer, output, tmp, 0)
}

// QueueController maintains a queue of commands to run (up to capacity).
type QueueController struct {
	inv           *Invoker
	statusManager *StatusManager
	capacity      int

	mu           sync.Mutex
	queue        []cmdAndID
	runningID    runner.RunID
	runningAbort chan<- struct{}
}

// Run enqueues the command or rejects it, returning its status or an error.
func (c *QueueController) Run(cmd *runner.Command) (runner.RunStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	st, err := c.statusManager.NewRun()
	if err != nil {
		return st, err
	}
	if c.runningID == runner.RunID("") {
		c.start(cmd, st.RunID)
	} else {
		if len(c.queue) >= c.capacity {
			return runner.RunStatus{}, fmt.Errorf(QueueFullMsg)
		}
		c.queue = append(c.queue, cmdAndID{cmd, st.RunID})
	}
	return st, nil
}

// Abort kills the given run, returning its final status.
func (c *QueueController) Abort(run runner.RunID) (runner.RunStatus, error) {
	c.mu.Lock()

	if run == c.runningID {
		if c.runningAbort != nil {
			close(c.runningAbort)
			c.runningAbort = nil
		}
	} else {
		for i, cmdAndID := range c.queue {
			if run == cmdAndID.id {
				c.queue = append(c.queue[:i], c.queue[i+1:]...)
				c.statusManager.Update(runner.AbortStatus(run))
			}
		}
	}

	// Unlock so watch() abort can call c.statusManager.Update()
	c.mu.Unlock()
	return runner.FinalStatus(c.statusManager, run)
}

// start starts a command, returning the current status
func (c *QueueController) start(cmd *runner.Command, id runner.RunID) {
	c.runningID = id
	abortCh, updateCh := c.inv.Run(cmd, id)
	c.runningAbort = abortCh
	go c.watch(updateCh)
}

func (c *QueueController) watch(updateCh <-chan runner.RunStatus) {
	for st := range updateCh {
		log.Info("Queue pulled result:%+v\n", st)
		if st.State.IsDone() {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.runningID = ""
			c.runningAbort = nil
			if len(c.queue) > 0 {
				cmdAndID := c.queue[0]
				c.queue = c.queue[1:]
				log.Info("Running from queue:%+v\n", cmdAndID)
				c.start(cmdAndID.cmd, cmdAndID.id)
			}
		}
		c.statusManager.Update(st)
	}
}
