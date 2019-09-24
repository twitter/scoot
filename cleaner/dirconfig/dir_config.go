package dirconfig

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

// A DirConfig defines the manner in which a particular directory should be cleaned
type DirConfig interface {
	CleanDir() error
	GetDir() string
}

// use posix du to get disk usage of a dir, for simplicity vs syscall or walking dir contents
func getDiskUsageKB(dir string) (uint64, error) {
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
