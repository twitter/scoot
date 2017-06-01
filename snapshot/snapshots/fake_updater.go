package snapshots

import (
	"sync"
	"time"

	"github.com/scootdev/scoot/snapshot"
)

// Implements snapshots.Updater
// Simple updater that supports incrementing an external counter that
// can be used to verify whether an Update() has run.
// Supports "pause" to allow race-free control of execution
// When used with "pause", to step:
//  1. WaitForUpdateRunning() - wait for first update to run (paused)
//	2. Unpause() - first update continues (but we don't know that it's run yet)
//  3. WaitForUpdateRunning() - 1st update has finished, 2nd is running (paused)

func MakeCountingUpdater(p *int, i time.Duration, a bool) snapshot.Updater {
	updater := &CountingUpdater{pointer: p, interval: i, pause: a}
	if a {
		updater.inProg.Lock()
		updater.wg.Add(1)
	}
	return updater
}

type CountingUpdater struct {
	pointer  *int
	interval time.Duration
	pause    bool
	wg       sync.WaitGroup
	crit     sync.Mutex // critical section, locked while update is running including pause
	inProg   sync.Mutex // locked while we are NOT running, grabbing this means an update started
}

func (c *CountingUpdater) Update() error {
	if c.pause {
		c.crit.Lock()
		c.inProg.Unlock()
		c.wg.Wait()
	}

	*c.pointer += 1

	if c.pause {
		c.wg.Add(1)
		c.inProg.Unlock()
		c.crit.Unlock()
	}
	return nil
}

func (c *CountingUpdater) UpdateInterval() time.Duration {
	return c.interval
}

// Not part of interface. If CountingUpdater was created without pause, no effect
func (c *CountingUpdater) Unpause() {
	if c.pause {
		c.wg.Done()
		c.inProg.Lock()
	}
}

func (c *CountingUpdater) WaitForUpdateRunning() {
	if c.pause {
		c.inProg.Lock()
	}
}
