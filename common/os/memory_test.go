package os

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	scoot_exec "github.com/twitter/scoot/common/os/exec"
)

func TestGetUserMemory(t *testing.T) {
	memory := NewMemory(MakeUserMemoryFakeExecer(t))

	mem, err := memory.GetUserCurrentMem()

	assert.Nil(t, err)
	assert.Equal(t, 1111111, mem)
}

func MakeUserMemoryFakeExecer(t *testing.T) *scoot_exec.ValidatingExecer {
	user := os.Getenv("USER")
	ve := scoot_exec.NewValidatingExecer(t, [][]string{{"ps", "-eo", fmt.Sprintf("user:%d,rss=", len(user)), "|", "grep", user, "|", "tr", "'\n'", "';'', '|', 'sed', ''s,;$,,'"}})
	psOut := []byte("scoot-s+  1;scoot-s+  10;scoot-s+  100;scoot-s+  1000;scoot-s+  10000;scoot-s+  100000;scoot-s+  1000000")
	actionMap := map[int]func(cmd scoot_exec.Cmd) error{
		0: func(cmd scoot_exec.Cmd) error {
			tCmd, ok := cmd.(*scoot_exec.ValidatingCmd)
			if !ok {
				return fmt.Errorf("expected cmd to be *twexec.ValidatingCmd, instead it is %T.  The test is set up incorrectly", cmd)
			}
			tCmd.GetStdout().Write(psOut)
			return nil
		},
	}
	ve.SetFakeActions(actionMap)
	return ve
}
