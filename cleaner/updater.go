package cleaner

import (
	"time"
)

// This Updater is just a means to call cleanup at a set frequency for a worker that
// otherwise doesn't need Filer updates.

// An Updater for snapshot Filers that invokes a cleaner.Cleaner at the update interval.
// Implements github.com/wisechengyi/scoot/snapshot.Updater interface
type CleaningUpdater struct {
	interval time.Duration
	cleaner  Cleaner
}

func NewCleaningUpdater(interval time.Duration, cleaner Cleaner) *CleaningUpdater {
	return &CleaningUpdater{
		interval: interval,
		cleaner:  cleaner,
	}
}

func (u *CleaningUpdater) Update() error {
	if u.cleaner != nil {
		_ = u.cleaner.Cleanup()
		// We don't return Cleanup, as it often errors & creates noise
	}
	return nil
}

func (u *CleaningUpdater) UpdateInterval() time.Duration {
	return u.interval
}
