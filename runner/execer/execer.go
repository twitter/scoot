package execer

// Execer lets you run one Unix command. It differs from Runner in that it does not
// know about Snapshots or Scoot. It's just a way to run a Unix process (or fake it).
// It's at the level of os/exec, not exec-as-a-service.

type Command struct {
	Argv []string
	// TODO(dbentley): accept dir
	// TODO(dbentley): environment variables?
}

type ProcessState int

const (
	UNKNOWN ProcessState = iota
	RUNNING
	COMPLETE
	FAILED
)

func (s ProcessState) IsDone() bool {
	return s == COMPLETE || s == FAILED
}

type Execer interface {
	Exec(command Command) (Process, error)
}

type Process interface {
	// TODO(dbentley): perhaps have a poll method?
	Wait() ProcessStatus
	Abort() ProcessStatus
}

type ProcessStatus struct {
	State    ProcessState
	ExitCode int
	Error    string

	StdoutURI string
	StderrURI string
}
