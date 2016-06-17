package execer

type Command struct {
	Argv []string
	// TODO(dbentley): accept dir
	// TODO(dbentley): environment variables?
}

type Execer interface {
	Exec(command Cmd) (Process, error)
}

type Process interface {
	Wait() (ProcessState, error)
	Abort() (ProcessState, error)
}

type ProcessState struct {
	State    ProcessState
	ExitCode int
	Error    string

	StdoutURI string
	StderrURI string
}
