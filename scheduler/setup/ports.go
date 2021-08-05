package setup

import (
	"fmt"
	"net"
	"time"

	log "github.com/sirupsen/logrus"
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
	address := fmt.Sprintf("localhost:%d", port)
	end := time.Now().Add(timeout)
	for !time.Now().After(end) {
		conn, _ := net.Dial("tcp", address)
		if conn != nil {
			log.Infof("Port %v active", port)
			_ = conn.Close()
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("port %v is not up after %v", port, timeout)
}
