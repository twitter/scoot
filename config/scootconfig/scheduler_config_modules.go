package scootconfig

import (
	"time"

	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/sched/scheduler"
)

const DefaultRunnerRetryTimeout = 10 * time.Second // How long to keep retrying a runner req
const DefaultRunnerRetryInterval = time.Second     // How long to sleep between runner req retries.
const DefaultReadyFnBackoff = 5 * time.Second      // How long to wait between runner status queries to determine [init] status.

// Parameters to configure the Stateful Scheduler
// MaxRetriesPerTask - the number of times to retry a failing task before
//                     marking it as completed.
// DebugMode - if true, starts the scheduler up but does not start
//             the update loop.  Instead the loop must be advanced manulaly
//             by calling step()
// RecoverJobsOnStartup - if true, the scheduler recovers active sagas,
//             from the sagalog, and restarts them.
// DefaultTaskTimeoutMs - default timeout for tasks, in ms
// OverheadMs - default overhead to add (to account for network and downloading)
type StatefulSchedulerConfig struct {
	Type                 string
	MaxRetriesPerTask    int
	DebugMode            bool
	RecoverJobsOnStartup bool
	DefaultTaskTimeoutMs int
	RunnerOverheadMs     int
}

func (c *StatefulSchedulerConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

func (c *StatefulSchedulerConfig) Create() scheduler.SchedulerConfig {
	return scheduler.SchedulerConfig{
		MaxRetriesPerTask:    c.MaxRetriesPerTask,
		DebugMode:            c.DebugMode,
		RecoverJobsOnStartup: c.RecoverJobsOnStartup,
		DefaultTaskTimeout:   time.Duration(c.DefaultTaskTimeoutMs) * time.Millisecond,
		RunnerOverhead:       time.Duration(c.RunnerOverheadMs) * time.Millisecond,
		RunnerRetryTimeout:   DefaultRunnerRetryTimeout,
		RunnerRetryInterval:  DefaultRunnerRetryInterval,
		ReadyFnBackoff:       DefaultReadyFnBackoff,
	}
}
