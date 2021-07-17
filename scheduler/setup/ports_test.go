package setup

import (
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"
)

func Test_WaitForPort_OpenedPort(t *testing.T) {
	t.Parallel()

	listener, _ := net.Listen("tcp", "localhost:0")
	_, portStr, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portStr)
	defer func() {
		if err := listener.Close(); err != nil {
			t.Errorf("failed to close test socket: %s", err)
		}
	}()

	if err := WaitForPort(port); err != nil {
		t.Fatalf("WaitForPort failed: %s", err)
	}
}

func Test_WaitForPortTimeout_ClosedPort(t *testing.T) {
	t.Parallel()

	listener, _ := net.Listen("tcp", "localhost:0")
	_, portStr, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portStr)
	if err := listener.Close(); err != nil {
		t.Errorf("failed to close test socket: %s", err)
	}

	// OS is very unlikely to immediately allocate port we just vacated
	err := WaitForPortTimeout(port, 1*time.Millisecond)
	if err == nil || err.Error() != fmt.Sprintf("port %v is not up after 1ms", port) {
		t.Fatalf("expected error, got: %s", err)
	}
}
