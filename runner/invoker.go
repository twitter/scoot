package runner

// Invoker runs a Scoot command. This involves:
//   * setup (check out a snapshot)
//   * post-processing (save output)
//   * babysitting (implement timeout and abort)
//   * updates while running
type Invoker interface {
	// Run runs cmd as run id returning the final ProcessStatus
	// Run will send updates the process is running to updateCh.
	// Run will enforce cmd's Timeout, and will abort cmd if abortCh is signaled.
	// Run will not return until the process is not running (done, aborted, or not started).
	Run(cmd *Command, id RunId, abortCh chan struct{}, updateCh chan ProcessStatus) ProcessStatus
}
