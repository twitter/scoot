package runners

import (
	"fmt"
	"sync"

	log "github.com/Sirupsen/logrus"

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
// The init chan is optional and may be nil if the filer has no initializion step.
func NewQueueRunner(
	exec execer.Execer, filer snapshot.Filer, idc snapshot.InitDoneCh, output runner.OutputCreator, tmp *temp.TempDir, capacity int,
) runner.Service {
	statusManager := NewStatusManager()
	inv := NewInvoker(exec, filer, output, tmp)
	controller := &QueueController{statusManager: statusManager, inv: inv, initDoneCh: idc, capacity: capacity}
	run := &Service{controller, statusManager, statusManager}

	statusManager.UpdateService(runner.ServiceStatus{Initialized: idc == nil})
	if idc != nil {
		go func() {
			<-idc
			statusManager.UpdateService(runner.ServiceStatus{Initialized: true})
		}()
	} else {
		statusManager.UpdateService(runner.ServiceStatus{Initialized: true})
	}

	return run
}

func NewSingleRunner(
	exec execer.Execer, filer snapshot.Filer, idc snapshot.InitDoneCh, output runner.OutputCreator, tmp *temp.TempDir) runner.Service {
	return NewQueueRunner(exec, filer, idc, output, tmp, 0)
}

// QueueController maintains a queue of commands to run (up to capacity).
type QueueController struct {
	inv           *Invoker
	statusManager *StatusManager
	initDoneCh    snapshot.InitDoneCh
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

	// Note, 'capacity' is the max number of queued cmds and we allow one running job before
	//        we start queueing, so total allowed runs is actually defined as capacity+1.
	numCmds := 0
	isRunning := (c.runningID != runner.RunID(""))
	if isRunning {
		numCmds = 1 + len(c.queue)
	}
	ready := false
	select {
	case <-c.initDoneCh:
		ready = true
	default:
		ready = (c.initDoneCh == nil)
	}
	log.Infof("Trying to run, ready=%t, available slots:%d/%d, currentRun:%s cmd:%v",
		ready, c.capacity+1-numCmds, c.capacity+1, c.runningID, cmd)

	if !ready || numCmds > c.capacity {
		return runner.RunStatus{}, fmt.Errorf(QueueFullMsg)
	}
	st, err := c.statusManager.NewRun()

	if err != nil {
		return st, err
	}
	if !isRunning {
		c.start(cmd, st.RunID)
	} else {
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
	status, _, err := runner.FinalStatus(c.statusManager, run)
	return status, err
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
		log.Debugf("Queue pulled result:%+v\n", st)
		if st.State.IsDone() {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.runningID = ""
			c.runningAbort = nil
			if len(c.queue) > 0 {
				cmdAndID := c.queue[0]
				c.queue = c.queue[1:]
				log.Infof("Running from queue:%+v\n", cmdAndID)
				c.start(cmdAndID.cmd, cmdAndID.id)
			}
		}
		c.statusManager.Update(st)
	}
}
