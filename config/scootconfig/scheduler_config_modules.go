package scootconfig

import (
	"time"

	"github.com/twitter/scoot/ice"
	"github.com/twitter/scoot/sched/scheduler"
)

//TODO(jschiller): make these configurable.

// How long to keep retrying a runner req
const DefaultRunnerRetryTimeout = 10 * time.Second

// How long to sleep between runner req retries.
const DefaultRunnerRetryInterval = time.Second

// How long to wait between runner status queries to determine [init] status.
const DefaultReadyFnBackoff = 5 * time.Second

// Parameters to configure the Stateful Scheduler
// MaxRetriesPerTask - the number of times to retry a failing task before
//                     marking it as completed.
// DebugMode - if true, starts the scheduler up but does not start
//             the update loop.  Instead the loop must be advanced manulaly
//             by calling step()
// RecoverJobsOnStartup - if true, the scheduler recovers active sagas,
//             from the sagalog, and restarts them.
// DefaultTaskTimeout - default timeout for tasks, human readable ex: "30m"
//
// See scheduler.SchedulerConfig for comments on the remaining fields.
type StatefulSchedulerConfig struct {
	Type                    string
	MaxRetriesPerTask       int
	DebugMode               bool
	RecoverJobsOnStartup    bool
	DefaultTaskTimeout      string
	TaskTimeoutOverhead     string
	MaxRequestors           int
	MaxJobsPerRequestor     int
	SoftMaxSchedulableTasks int
}

func (c *StatefulSchedulerConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

func (c *StatefulSchedulerConfig) Create() (scheduler.SchedulerConfig, error) {
	var err error
	var dtt time.Duration
	if c.DefaultTaskTimeout != "" {
		dtt, err = time.ParseDuration(c.DefaultTaskTimeout)
		if err != nil {
			return scheduler.SchedulerConfig{}, err
		}
	}
	var tto time.Duration
	if c.TaskTimeoutOverhead != "" {
		tto, err = time.ParseDuration(c.TaskTimeoutOverhead)
		if err != nil {
			return scheduler.SchedulerConfig{}, err
		}
	}

	return scheduler.SchedulerConfig{
		MaxRetriesPerTask:       c.MaxRetriesPerTask,
		DebugMode:               c.DebugMode,
		RecoverJobsOnStartup:    c.RecoverJobsOnStartup,
		DefaultTaskTimeout:      dtt,
		TaskTimeoutOverhead:     tto,
		RunnerRetryTimeout:      DefaultRunnerRetryTimeout,
		RunnerRetryInterval:     DefaultRunnerRetryInterval,
		ReadyFnBackoff:          DefaultReadyFnBackoff,
		MaxRequestors:           c.MaxRequestors,
		MaxJobsPerRequestor:     c.MaxJobsPerRequestor,
		SoftMaxSchedulableTasks: c.SoftMaxSchedulableTasks,
	}, nil
}
