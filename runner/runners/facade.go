package runners

import (
	"github.com/scootdev/scoot/runner"
)

// ServiceFacade makes a Service from component parts.
type ServiceFacade struct {
	runner.Controller
	runner.StatusReader
	// TODO(dbentley): get rid of StatusEraser from here
	runner.StatusEraser
}
