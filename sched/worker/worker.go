package worker

type Command struct{}

type RunId string

type RunStatus struct{}

type WorkerStatus struct{}

type Worker interface {
	// Runs the command.
	// Blocks until the worker has accepted the command and either started running it or returned an error
	Run(cmd Command) (RunId, error)

	Query(run RunId) (RunStatus, error)

	WorkerStatus() (WorkerStatus, error)

	// Wait for run to be done
	Wait(run RunId) (RunStatus, error)
}
