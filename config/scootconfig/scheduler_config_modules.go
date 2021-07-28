package scootconfig

import (
	"strings"
	"time"

	"github.com/twitter/scoot/ice"
	"github.com/twitter/scoot/scheduler/server"
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
//             the update loop.  Instead the loop must be advanced manually
//             by calling step()
// RecoverJobsOnStartup - if true, the scheduler recovers active sagas,
//             from the sagalog, and restarts them.
// DefaultTaskTimeout - default timeout for tasks, human readable ex: "30m"
//
// See server.SchedulerConfiguration for comments on the remaining fields.
type StatefulSchedulerConfig struct {
	Type                 string
	MaxRetriesPerTask    int
	DebugMode            bool
	RecoverJobsOnStartup bool
	DefaultTaskTimeout   string
	TaskTimeoutOverhead  string
	MaxRequestors        int
	MaxJobsPerRequestor  int
	Admins               string
}

func (c *StatefulSchedulerConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

func (c *StatefulSchedulerConfig) Create() (server.SchedulerConfiguration, error) {
	var err error
	var dtt time.Duration
	if c.DefaultTaskTimeout != "" {
		dtt, err = time.ParseDuration(c.DefaultTaskTimeout)
		if err != nil {
			return server.SchedulerConfiguration{}, err
		}
	}
	var tto time.Duration
	if c.TaskTimeoutOverhead != "" {
		tto, err = time.ParseDuration(c.TaskTimeoutOverhead)
		if err != nil {
			return server.SchedulerConfiguration{}, err
		}
	}
	admins := []string{}
	for _, admin := range strings.Split(c.Admins, ",") {
		if admin != "" {
			admins = append(admins, admin)
		}
	}

	return server.SchedulerConfiguration{
		MaxRetriesPerTask:    c.MaxRetriesPerTask,
		DebugMode:            c.DebugMode,
		RecoverJobsOnStartup: c.RecoverJobsOnStartup,
		DefaultTaskTimeout:   dtt,
		TaskTimeoutOverhead:  tto,
		RunnerRetryTimeout:   DefaultRunnerRetryTimeout,
		RunnerRetryInterval:  DefaultRunnerRetryInterval,
		ReadyFnBackoff:       DefaultReadyFnBackoff,
		MaxRequestors:        c.MaxRequestors,
		MaxJobsPerRequestor:  c.MaxJobsPerRequestor,
		Admins:               admins,
	}, nil
}
