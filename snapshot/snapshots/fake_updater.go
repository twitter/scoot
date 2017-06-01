package snapshots

import (
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

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

func MakeCountingUpdater(i *int, d time.Duration, p bool) snapshot.Updater {
	updater := &CountingUpdater{pointer: i, interval: d, pause: p}
	if p {
		updater.wg.Add(1)
	}
	return updater
}

type CountingUpdater struct {
	pointer  *int
	interval time.Duration
	pause    bool

	// wg blocks an in-progress update until Unpause()'d
	// mu is locked for the duration of an Update() including pause
	wg       sync.WaitGroup
	mu       sync.Mutex
}

func (c *CountingUpdater) Update() error {
	if c.pause {
		// grab the lock
		c.mu.Unlock()
		c.mu.Lock()

		// wait for Unpause
		c.wg.Wait()
	}

	log.Infof("CountingUpdater - Update and increment var %v (current: %d)\n", c.pointer, *c.pointer)
	*c.pointer += 1

	if c.pause {
		// reset Unpause and release mu
		c.wg.Add(1)
		c.mu.Unlock()
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
	}
}

// Not part of interface.
func (c *CountingUpdater) WaitForUpdateRunning() {
	if c.pause {
		c.mu.Lock()
	}
}
