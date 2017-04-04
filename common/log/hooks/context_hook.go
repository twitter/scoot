package hooks

import (
	"runtime/debug"
	"strings"

	"github.com/Sirupsen/logrus"
)

type contextHook struct {
}

func NewContextHook() contextHook {
	return contextHook{}
}

func (hook contextHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (hook contextHook) Fire(entry *logrus.Entry) error {
	stack := debug.Stack()
	lines := strings.Split(string(stack), "\n")
	foundLoggerBlock := false
	incr := 1
	for i := 0; i < len(lines); i = i + incr {
		if strings.Contains(lines[i], "context_hook.go:") {
			foundLoggerBlock = true
			incr = 2
			continue
		}
		if !foundLoggerBlock {
			continue
		}
		ctx := strings.Split(lines[i], "scoot/")
		entry.Data["file:line"] = strings.TrimSpace(ctx[len(ctx)-1])
	}
	return nil
}
