package stats

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

// DiskMonitor monitor disk usage for selected directories
type DiskMonitor struct {
	shortName  []string
	paths      []string
	startSizes []int64
	endSizes   []int64
}

// NewDiskMonitor return a DiskMonitor
func NewDiskMonitor(shortNames []string, paths []string) *DiskMonitor {
	dm := &DiskMonitor{shortName: shortNames, paths: paths, startSizes: make([]int64, len(paths)), endSizes: make([]int64, len(paths))}
	for i := range paths {
		dm.startSizes[i] = -1
		dm.endSizes[i] = -1
	}
	return dm
}

// GetStartSizes get the starting sizes of the directories being monitored
func (dm *DiskMonitor) GetStartSizes() {
	dm.getSizes(true)
}

// GetEndSizes get the ending sizes of the directories being monitored
func (dm *DiskMonitor) GetEndSizes() {
	dm.getSizes(false)
}

// RecordSizeStats record the disk size deltas to the stats receiver
func (dm *DiskMonitor) RecordSizeStats(stat StatsReceiver) {
	for i, shortName := range dm.shortName {
		delta := dm.endSizes[i] - dm.startSizes[i]
		statName := fmt.Sprintf("%s_%s", WorkerDirSizeChange, shortName)
		stat.Gauge(statName).Update(delta)
	}
}

// getStartSizes get the starting sized of the directories being monitored
func (dm *DiskMonitor) getSizes(start bool) {
	var err error
	for i, p := range dm.paths {
		var dSize uint64
		var asInt int64
		dSize, err = GetDiskUsageKB(p)
		if err != nil {
			log.Errorf("error getting disk size for %s, will not monitor size: %s", dm.paths[i], err)
			asInt = -1
		} else {
			asInt = int64(dSize)
		}
		if start {
			dm.startSizes[i] = asInt
		} else {
			dm.endSizes[i] = asInt
		}
	}
}

// GetDiskUsageKB use posix du to get disk usage of a dir, for simplicity vs syscall or walking dir contents
func GetDiskUsageKB(dir string) (uint64, error) {
	// shortcut if dir not exist
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return 0, nil
	}

	name := "du"
	args := []string{"-sk", dir}

	stdout, err := exec.Command(name, args...).Output()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			log.Errorf("Failed to run %q %q: %s. Stderr: %s\n", name, args, err, exitError.Stderr)
		}
		return 0, err
	}

	s := string(stdout)
	arr := strings.Fields(s)
	if len(arr) != 2 {
		return 0, errors.New(fmt.Sprintf("Unexpected output from %s: %q (Expected \"<kb> <dir>\")", name, s))
	}

	return strconv.ParseUint(arr[0], 10, 64)
}
