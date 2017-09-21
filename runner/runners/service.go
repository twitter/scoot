package runners

import (
	"github.com/twitter/scoot/runner"
)

// Service makes a runner.Service from component parts.
type Service struct {
	runner.Controller
	runner.StatusReader
	// TODO(dbentley): get rid of StatusEraser from here
	runner.StatusEraser
}
