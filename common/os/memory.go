package os

import (
	"fmt"
	"os"
	"strings"

	mem_exec "github.com/twitter/scoot/common/os/exec"
)

type Memory struct {
	execer mem_exec.OsExec
}

func NewMemory(execer mem_exec.OsExec) *Memory {
	return &Memory{execer: execer}
}

// GetUserCurrentMem compute the memory (rss) used by all processes owned by the current user
func (m *Memory) GetUserCurrentMem() (int, error) {
	user := os.Getenv("USER")
	cmd := m.execer.Command("ps", "-eo", fmt.Sprintf("user:%d,rss=", len(user)), "|", "grep", user, "|", "tr", "'\n'", "';'', '|', 'sed', ''s,;$,,'")
	b, err := cmd.Output()
	if err != nil {
		return 0, err
	}
	totalMem := 0
	procs := strings.Split(string(b), ";")
	for idx := 0; idx < len(procs); idx += 1 {
		var user string
		var mem int
		n, err := fmt.Sscanf(procs[idx], "%s %d", &user, &mem)
		if err != nil {
			return 0, err
		}
		if n != 2 {
			return 0, fmt.Errorf("error parsing output, expected 2 assignments, but only received %d. %s. %s", n, procs[idx], err)
		}
		totalMem += mem
	}
	return totalMem, nil
}
