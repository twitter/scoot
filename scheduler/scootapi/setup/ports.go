package setup

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os/exec"
	"strconv"
	"time"
)

// WaitForPort waits 10 seconds for a process to listen to the port, and returns an error if
// the port remains open
func WaitForPort(port int) error {
	return WaitForPortTimeout(port, 10*time.Second)
}

// WaitForPortTimeout waits timeout for a process to listen to the port, and returns an error if
// the port remains open
func WaitForPortTimeout(port int, timeout time.Duration) error {
	log.Infof("Waiting for port %v for %v", port, timeout)
	end := time.Now().Add(timeout)
	for !time.Now().After(end) {
		// Use exec.Command because we don't worry about these getting orphaned,
		// and don't want to fill up our Cmds's list of running cmds
		cmd := exec.Command("nc", "-z", "localhost", strconv.Itoa(port))
		if err := cmd.Run(); err == nil {
			log.Infof("Port %v active", port)
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("port %v is not up after 5s", port)
}
