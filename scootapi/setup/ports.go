package setup

import (
	"fmt"
	"log"
	"os/exec"
	"time"
)

// WaitForPort waits ~5 seconds for a process to listen to the port, and returns an error if
// the port remains open
func WaitForPort(port string) error {
	log.Printf("Waiting for port %v", port)
	for i := 0; i < 10; i++ {
		// Use exec.Command because we don't worry about these getting orphaned,
		// and don't want to fill up our Cmds's list of running cmds
		cmd := exec.Command("nc", "-z", "localhost", port)
		if err := cmd.Run(); err == nil {
			log.Printf("Port %v active", port)
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("port %v is not up after 5s", port)
}
