package runners

import (
	"github.com/scootdev/scoot/runner"
)

// Create a new ControllerAndStatuserRunner than uses cont for the controller and stat for the Statuser
func NewControllerAndStatuserRunner(cont runner.Controller, stat runner.Statuser) *ControllerAndStatuserRunner {
	return &ControllerAndStatuserRunner{cont, stat}
}

// ControllerAndStatuserRunner combines a Controller and a Statuser to make a Runner.
type ControllerAndStatuserRunner struct {
	cont runner.Controller
	stat runner.Statuser
}

// Run runs by calling controller.Run
func (r *ControllerAndStatuserRunner) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
	return r.cont.Run(cmd)
}

// Abort aborts by calling controller.Abort
func (r *ControllerAndStatuserRunner) Abort(run runner.RunId) (runner.ProcessStatus, error) {
	return r.cont.Abort(run)
}

// StatusQuery queries Status by calling statuses.StatusQuery
func (r *ControllerAndStatuserRunner) StatusQuery(q runner.StatusQuery, opts runner.PollOpts) ([]runner.ProcessStatus, error) {
	return r.stat.StatusQuery(q, opts)
}

// StatusQuerySingle queries a single Status by calling statuses.StatusQuerySingl
func (r *ControllerAndStatuserRunner) StatusQuerySingle(q runner.StatusQuery, opts runner.PollOpts) (runner.ProcessStatus, error) {
	return r.stat.StatusQuerySingle(q, opts)
}

// Status returns a status by calling statuses.Status
func (r *ControllerAndStatuserRunner) Status(run runner.RunId) (runner.ProcessStatus, error) {
	return r.stat.Status(run)
}

// StatusAll returns all statuses by calling statuses.StatusAll
func (r *ControllerAndStatuserRunner) StatusAll() ([]runner.ProcessStatus, error) {
	return r.stat.StatusAll()
}

// Erase erases by calling statuses.Erase
func (r *ControllerAndStatuserRunner) Erase(run runner.RunId) error {
	return r.stat.Erase(run)
}
