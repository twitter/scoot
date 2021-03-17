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

var NopDirsMonitor *DirsMonitor = NewDirsMonitor([]MonitorDir{})

// MonitorDir a directory to monitor and a shortname (suffix) for reporting the stat
type MonitorDir struct {
	Directory  string // the directory to monitor
	StatSuffix string // the suffix to use on the commandDirUsage_kb stat
	startSize  int64
	endSize    int64
}

// DirsMonitor monitor disk usage for selected directories
type DirsMonitor struct {
	dirs []MonitorDir
}

// NewDirsMonitor return a DirsMonitor
func NewDirsMonitor(dirs []MonitorDir) *DirsMonitor {
	return &DirsMonitor{dirs: dirs}
}

// GetStartSizes get the starting sizes of the directories being monitored
func (dm *DirsMonitor) GetStartSizes() {
	dm.getSizes(true)
}

// GetEndSizes get the ending sizes of the directories being monitored
func (dm *DirsMonitor) GetEndSizes() {
	dm.getSizes(false)
}

// RecordSizeStats record the disk size deltas to the stats receiver
func (dm *DirsMonitor) RecordSizeStats(stat StatsReceiver) {
	for key := range dm.dirs {
		dir := &(dm.dirs[key])
		if dir.startSize != -1 && dir.endSize != -1 {
			delta := dir.endSize - dir.startSize
			statName := fmt.Sprintf("%s_%s", CommandDirUsageKb, dir.StatSuffix)
			stat.Gauge(statName).Update(delta)
		}
	}
}

// getStartSizes get the starting sized of the directories being monitored
func (dm *DirsMonitor) getSizes(isStart bool) {
	var err error
	for key := range dm.dirs {
		var dSize uint64
		var asInt int64
		dir := &(dm.dirs[key])
		dSize, err = GetDiskUsageKB(dir.Directory)
		if err != nil {
			log.Errorf("error getting disk size for %s, will not monitor size: %s", dir.Directory, err)
			asInt = -1
		} else {
			asInt = int64(dSize)
		}
		if isStart {
			dir.startSize = asInt
		} else {
			dir.endSize = asInt
		}
	}
}

// GetDiskUsageKB use posix du to get disk usage of a dir, for simplicity vs syscall or walking dir contents
func GetDiskUsageKB(dir string) (uint64, error) {
	// shortcut if dir not exist
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		cwd, _ := os.Getwd()
		log.Infof("GetDiskUsageKB, directory %s does not exist in %s: return 0kb", dir, cwd)
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
