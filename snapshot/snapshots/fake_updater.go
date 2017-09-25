package snapshots

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/snapshot"
)

// Implements snapshots.Updater
// Simple updater that supports incrementing an external counter that
// can be used to verify whether an Update() has run.
// Supports "pause" to allow race-free control of execution
// When used with "pause", to step:
//  1. WaitForUpdateRunning() - wait for first update to run (paused)
//	2. Unpause() - first update continues (but we don't know that it's run yet)
//  3. WaitForUpdateRunning() - 1st update has finished, 2nd is running (paused)

func MakeCountingUpdater(i *int, d time.Duration, p bool) snapshot.Updater {
	updater := &CountingUpdater{pointer: i, interval: d, pause: p}
	if p {
		updater.upRunCh = make(chan struct{})
		updater.pauseCh = make(chan struct{})
	}
	return updater
}

type CountingUpdater struct {
	pointer  *int
	interval time.Duration
	pause    bool

	upRunCh chan struct{}
	pauseCh chan struct{}
}

// update waits on unpauseCh
// Unpause just sends to unpauseCh (which means Update() should be running when we hit unpause)
// WFUR should wait on an updateRunningCh, that Update() sends to.
func (c *CountingUpdater) Update() error {
	if c.pause {
		c.upRunCh <- struct{}{}
		<-c.pauseCh
	}

	log.Infof("CountingUpdater - Update and increment var %v (current: %d)\n", c.pointer, *c.pointer)
	*c.pointer += 1
	return nil
}

func (c *CountingUpdater) UpdateInterval() time.Duration {
	return c.interval
}

// Not part of interface. If CountingUpdater was created without pause, no effect

func (c *CountingUpdater) Unpause() {
	if c.pause {
		c.pauseCh <- struct{}{}
	}
}

func (c *CountingUpdater) WaitForUpdateRunning() {
	if c.pause {
		<-c.upRunCh
	}
}
