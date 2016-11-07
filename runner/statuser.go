package runner

type StateMask int64

const (
	UNKNOWN_MASK    StateMask = 1 << UNKNOWN
	PENDING_MASK              = 1 << PENDING
	PREPARING_MASK            = 1 << PREPARING
	RUNNING_MASK              = 1 << RUNNING
	COMPLETE_MASK             = 1 << COMPLETE
	FAILED_MASK               = 1 << FAILED
	ABORTED_MASK              = 1 << ABORTED
	TIMEDOUT_MASK             = 1 << TIMEDOUT
	BADREQUEST_MASK           = 1 << BADREQUEST
)

type StatusQuery struct {
	Runs    []RunId
	AllRuns bool
	States  StateMask
}

type Statuser interface {
	StatusQuery(q StatusQuery, opts PollOpts) ([]ProcessStatus, error)

	// Convenience Function
	StatusQuerySingle(q StatusQuery, opts PollOpts) (ProcessStatus, error)

	// Legacy
	Status(run RunId) (ProcessStatus, error)
	StatusAll() ([]ProcessStatus, error)

	// Prunes the run history so StatusAll() can return a reasonable number of runs.
	Erase(run RunId) error
}

func (m StateMask) Matches(st ProcessStatus) bool {
	return (1 << st.State) & m
}

func (q StatusQuery) Matches(st ProcessStatus) bool {
	if !q.States.Matches(st) {
		return false
	}

	if q.AllRuns {
		return true
	}

	for _, id := range q.Runs {
		if id == st.RunId {
			return true
		}
	}

	return false
}
