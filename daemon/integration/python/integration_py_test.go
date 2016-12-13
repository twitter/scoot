package integration_py_test

import (
	"os"
	"os/exec"
	"testing"
)

// each test starts and stops the daemon, run the tests one at a time
func TestPython(t *testing.T) {
	cmd := exec.Command("../../protocol/python/tests/integration.py")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("TestPython failed: %v", err)
	}

	cmd = exec.Command("../../protocol/python/tests/daemon_cli_test.py")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("TestPythonCli failed: %v", err)
	}

	cmd = exec.Command("../../protocol/python/tests/run_many.py")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("TestPythonCli failed: %v", err)
	}
}
