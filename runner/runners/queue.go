package runners

import (
	"fmt"
	"sync"

	"github.com/scootdev/scoot/runner"
)

type cmdAndID struct {
	cmd *runner.Command
	id  runner.RunID
}

// NewQueueController creates a new QueueController
func NewQueueController() *QueueController {
	return &QueueController{}
}

// QueueController maintains a queue of commands to run (up to capacity).
type QueueController struct {
	inv          *Invoker
	queue        []cmdAndID
	int          capacity
	runningID    runner.RunID
	runningAbort chan<- struct{}
	mu           sync.Mutex
}

// Run enqueues the command or rejects it, returning its status or an error.
func (c *QueueController) Run(cmd *runner.Command) (runner.RunStatus, error) {
	return runner.RunStatus{}, fmt.Errorf("not yet implemented")
}

// Abort kills the given run, returning its final status.
func (c *QueueController) Abort(run runner.RunID) (runner.RunStatus, error) {
	return runner.RunStatus{}, fmt.Errorf("not yet implemented")
}
