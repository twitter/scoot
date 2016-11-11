package runner

// Controller controls runs, implementing tenancy and queuing (or not).
type Controller interface {
	// Run instructs the Runner to run cmd. It returns its status and any errors.
	// Run may:
	// enqueue cmd (leading to state PENDING)
	// run cmd immediately (leading to state RUNNING)
	// check if cmd is well-formed, and reject it if not (leading to state FAILED)
	// wait a very short period of time for cmd to finish
	// Run may not wait indefinitely for cmd to finish. This is an async API.
	Run(cmd *Command) (ProcessStatus, error)

	// Kill the given run.
	Abort(run RunId) (ProcessStatus, error)
}
