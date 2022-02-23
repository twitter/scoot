package dirconfig

import (
	"errors"
	"fmt"
	"os/exec"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/stats"
)

// Configuration for cleaning disk space of a directory
// based on usage thresholds given as low and high watermarks in KB,
// and retention settings for each threshold as time since last modified in minutes.
// if LowMarkKB <= usage < HighMarkKB, prune files last modified prior to LowRetentionMin
// if HighMarkKB <= usage, prune files last modified prior to HighRetentionMin
type lastModifiedDirConfig struct {
	Dir              string
	LowMarkKB        uint64
	LowRetentionMin  uint
	HighMarkKB       uint64
	HighRetentionMin uint
}

func NewLastModifiedDirConfig(dir string, lowMarkKB uint64, lowRetentionMin uint, highMarkKB uint64, highRetentionMin uint) (*lastModifiedDirConfig, error) {
	if lowMarkKB >= highMarkKB {
		return nil, errors.New(
			fmt.Sprintf("Invalid DirConfig for %s: LowMarkKB %d >= HighMarkKB %d", dir, lowMarkKB, highMarkKB))
	}
	return &lastModifiedDirConfig{
		Dir:              dir,
		LowMarkKB:        lowMarkKB,
		LowRetentionMin:  lowRetentionMin,
		HighMarkKB:       highMarkKB,
		HighRetentionMin: highRetentionMin,
	}, nil
}

func (dc lastModifiedDirConfig) GetDir() string { return dc.Dir }

func (dc lastModifiedDirConfig) CleanDir() error {
	var usage uint64 = 0
	var err error = nil

	if dc.LowMarkKB != 0 || dc.HighMarkKB != 0 {
		usage, err = stats.GetDiskUsageKB(dc.GetDir())
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to Cleanup dir: %s. %s", dc.GetDir(), err))
		}
	}

	if usage >= dc.LowMarkKB && usage < dc.HighMarkKB {
		err = dc.cleanDir(dc.LowRetentionMin)
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to Cleanup dir: %s. %s", dc.GetDir(), err))
		}
	} else if usage >= dc.HighMarkKB {
		err = dc.cleanDir(dc.HighRetentionMin)
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to Cleanup dir: %s. %s", dc.GetDir(), err))
		}
	}
	log.Infof("Not cleaning %s, usage %d(KB) under threshold %d(KB)\n", dc.GetDir(), usage, dc.LowMarkKB)
	return nil
}

func (dc lastModifiedDirConfig) cleanDir(retentionMin uint) error {
	name := "find"
	args := []string{dc.GetDir(), "!", "-path", dc.GetDir(), "-mmin", fmt.Sprintf("+%d", retentionMin), "-delete"}

	log.Infof("Running cleanup for %s with cmd: %s %s\n", dc.GetDir(), name, args)
	_, err := exec.Command(name, args...).Output()
	if err != nil {
		log.Errorf("Error running cleanup command (this can commonly fail due to non-empty directories): %s\n", err)
		if errExit, ok := err.(*exec.ExitError); ok {
			log.Errorf("Cleanup command stderr:\n%s\n", errExit.Stderr)
		}
		return err
	}

	return nil
}
