// Package cleaner provides internal cleanup-related utilities,
// primarily to limit disk consumption in long-lived workers.
package cleaner

import (
	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/cleaner/dirconfig"
)

// A Cleaner provides cleanup functionality
type Cleaner interface {
	Cleanup() error
}

// Implements Cleaner for doing disk cleanup of directories
type DiskCleaner struct {
	DirConfigs []dirconfig.DirConfig
}

func NewDiskCleaner(dirConfigs []dirconfig.DirConfig) (*DiskCleaner, error) {
	return &DiskCleaner{
		DirConfigs: dirConfigs,
	}, nil
}

// Cleanup is performed on dirs specified in the DirConfigs.
// Cleanup does not guarantee usage at or below configured thresholds after running.
func (d *DiskCleaner) Cleanup() error {
	var failures []error
	for _, dc := range d.DirConfigs {
		if err := dc.CleanDir(); err != nil {
			failures = append(failures, err)
		}
	}
	if l := len(failures); l > 0 {
		log.Errorf("Failed to clean %d dir(s). %s", l, failures)
	}
	return nil
}
