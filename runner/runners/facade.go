package runners

import (
	"github.com/scootdev/scoot/runner"
)

// NewQueueRunner creates a new Service that uses a Queue
// (This is what the Daemon and Worker will call for now, and it will take an Execer, Filer, &c.)
func NewQueueRunner() runner.Service {
	controller := NewQueueController()
	statuses := NewStatuses()
	return &ServiceFacade{controller, statuses, statuses}
}

// ServiceFacade makes a Service from component parts.
type ServiceFacade struct {
	runner.Controller
	runner.StatusReader
	// TODO(dbentley): get rid of StatusEraser form here
	runner.StatusEraser
}
