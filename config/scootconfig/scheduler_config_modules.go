package scootconfig

import (
	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/sched/scheduler"
)

// Parameters to configure the Stateful Scheduler
// MaxRetriesPerTask - the number of times to retry a failing task before
//                     marking it as completed.
// DebugMode - if true, starts the scheduler up but does not start
//             the update loop.  Instead the loop must be advanced manulaly
//             by calling step()
// RecoverJobsOnStartup - if true, the scheduler recovers active sagas,
//             from the sagalog, and restarts them.
type StatefulSchedulerConfig struct {
	Type                 string
	MaxRetriesPerTask    int
	DebugMode            bool
	RecoverJobsOnStartup bool
}

func (c *StatefulSchedulerConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

func (c *StatefulSchedulerConfig) Create() scheduler.SchedulerConfig {
	return scheduler.SchedulerConfig{
		MaxRetriesPerTask:    c.MaxRetriesPerTask,
		DebugMode:            c.DebugMode,
		RecoverJobsOnStartup: c.RecoverJobsOnStartup,
	}
}
