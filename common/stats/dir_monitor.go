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

var NopDirMonitor *DirMonitor = NewDirMonitor([]MonitorDir{})

// MonitorDir a directory to monitor and a shortname (suffix) for reporting the stat
type MonitorDir struct {
	Directory  string // the directory to monitor
	StatSuffix string // the suffix to use on the commandDirUsage_kb stat
}

// DirMonitor monitor disk usage for selected directories
type DirMonitor struct {
	dirs       []MonitorDir
	startSizes []int64
	endSizes   []int64
}

// NewDirMonitor return a DirMonitor
func NewDirMonitor(dirs []MonitorDir) *DirMonitor {
	dm := &DirMonitor{dirs: dirs, startSizes: make([]int64, len(dirs)), endSizes: make([]int64, len(dirs))}
	for i := range dirs {
		dm.startSizes[i] = -1
		dm.endSizes[i] = -1
	}
	return dm
}

// GetStartSizes get the starting sizes of the directories being monitored
func (dm *DirMonitor) GetStartSizes() {
	dm.getSizes(true)
}

// GetEndSizes get the ending sizes of the directories being monitored
func (dm *DirMonitor) GetEndSizes() {
	dm.getSizes(false)
}

// RecordSizeStats record the disk size deltas to the stats receiver
func (dm *DirMonitor) RecordSizeStats(stat StatsReceiver) {
	for i, dir := range dm.dirs {
		delta := dm.endSizes[i] - dm.startSizes[i]
		statName := fmt.Sprintf("%s_%s", CommandDirUsageKb, dir.StatSuffix)
		stat.Gauge(statName).Update(delta)
	}
}

// getStartSizes get the starting sized of the directories being monitored
func (dm *DirMonitor) getSizes(isStart bool) {
	var err error
	for i, dir := range dm.dirs {
		var dSize uint64
		var asInt int64
		dSize, err = GetDiskUsageKB(dir.Directory)
		if err != nil {
			log.Errorf("error getting disk size for %s, will not monitor size: %s", dir.Directory, err)
			asInt = -1
		} else {
			asInt = int64(dSize)
		}
		if isStart {
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
