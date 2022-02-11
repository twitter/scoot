package dirconfig

import (
	"errors"
	"fmt"
	"os/exec"

	log "github.com/sirupsen/logrus"
	"github.com/twitter/scoot/common/stats"
)

// Configuration for cleaning disk space of a directory
// based purely on a maximum usage threshold.
// When exceeded, the entire directory will be removed.
type EntireDirConfig struct {
	Dir        string
	MaxUsageKB uint64
}

func (dc EntireDirConfig) GetDir() string { return dc.Dir }

func (dc EntireDirConfig) CleanDir() error {
	usage, err := stats.GetDiskUsageKB(dc.GetDir())
	if err != nil {
		return err
	}
	if usage > dc.MaxUsageKB {
		err = dc.cleanDir()
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to Cleanup dir: %s. %s", dc.GetDir(), err))
		}
	}
	return nil
}

func (dc EntireDirConfig) cleanDir() error {
	err := dc.changeDirPermsForDeletion()
	if err != nil {
		return err
	}

	name := "rm"
	args := []string{"-rf", dc.GetDir()}

	log.Infof("Running cleanup for %s with cmd: %s %s\n", dc.GetDir(), name, args)
	_, err = exec.Command(name, args...).Output()
	if err != nil {
		log.Errorf("Error running cleanup command (this can commonly fail due to non-empty directories): %s\n", err)
		if errExit, ok := err.(*exec.ExitError); ok {
			log.Errorf("Cleanup command stderr:\n%s\n", errExit.Stderr)
		}
		return err
	}

	return nil
}

func (dc EntireDirConfig) changeDirPermsForDeletion() error {
	name := "chmod"
	args := []string{"-R", "u+rwx", dc.GetDir()}

	log.Infof("Changing permissions on %s with cmd: %s %s\n", dc.GetDir(), name, args)
	_, err := exec.Command(name, args...).Output()
	if err != nil {
		if errExit, ok := err.(*exec.ExitError); ok {
			log.Errorf("Change dir permissions command stderr:\n%s\n", errExit.Stderr)
		}
		return err
	}

	return nil
}
