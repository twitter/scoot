package runners

import (
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/snapshot"
)

// NewInvoker creates an Invoker that will use the supplied helpers
func NewInvoker() *Invoker {
}

// TODO(dbentley): test this separately from the end-to-end runner tests

// Invoker Runs a Scoot Command by performing the Scoot setup and gathering.
// (E.g., checking out a Snapshot, or saving the Output once it's done)
// Unlike a full Runner, it has no idea of what else is running or has run.
type Invoker struct {
	exec   execer.Execer
	filer  snapshot.Filer
	output runner.OutputCreator
}

// Run runs cmd as run id
// Run will send updates as the process is running to updateCh.
// The RunStatus'es that come out of updateCh will have an empty RunID
// Run will enforce cmd's Timeout, and will abort cmd if abortCh is signaled.
// updateCh will not close until the run is finished running.
func (inv *Invoker) Run(cmd *runner.Command) (abortCh chan<- struct{}, updateCh <-chan runner.RunStatus) {
	return nil, nil
}
