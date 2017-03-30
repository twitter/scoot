package fuse

import (
	"bufio"
	"errors"
	"github.com/scootdev/scoot/common/log"
	"io"
	"sync"
)

var (
	// ErrOSXFUSENotFound is returned from Mount when the OSXFUSE
	// installation is not detected.
	//
	// Only happens on OS X. Make sure OSXFUSE is installed, or see
	// OSXFUSELocations for customization.
	ErrOSXFUSENotFound = errors.New("cannot locate OSXFUSE")
)

func neverIgnoreLine(line string) bool {
	return false
}

func lineLogger(wg *sync.WaitGroup, prefix string, ignore func(line string) bool, r io.ReadCloser) {
	defer wg.Done()

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if ignore(line) {
			continue
		}
		log.Info("%s: %s", prefix, line)
	}
	if err := scanner.Err(); err != nil {
		log.Info("%s, error reading: %v", prefix, err)
	}
}
