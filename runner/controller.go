package runner

type Controller interface {
	// Run prepares cmd. It returns its status and any errors.
	// Run may:
	// enqueue cmd (leading to state PENDING)
	// run cmd immediately (leading to state RUNNING)
	// check if cmd is well-formed, and reject it if not (leading to state FAILED)
	// wait a very short period of time for cmd to start (leading to any state)
	// Run may not wait indefinitely for cmd to finish. This is an async API.
	Run(cmd *Command) (RunStatus, error)

	// Abort kills the given run. Returns its final status (which may not be aborted,
	// e.g. if it's already completed on its own)
	Abort(run RunID) (RunStatus, error)

	// Optional function to clean up process-local resources, ex: cancel goroutines, close connections, etc
	Release()
}
