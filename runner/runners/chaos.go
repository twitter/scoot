package runners

import (
	"math/rand"
	"sync"
	"time"

	"github.com/scootdev/scoot/runner"
)

// Creates a new Chaos Runner
func NewChaosRunner(delC runner.Controller, delSts runner.LegacyStatuses) *ChaosRunner {
	return &ChaosRunner{delC: delC, delSts: delSts}
}

// ChaosRunner implements Runner by calling to a delegate runner in the happy path,
// but delaying a random time between 0 and MaxDelay, or returning an error
type ChaosRunner struct {
	delC     runner.Controller
	delSts   runner.LegacyStatuses
	mu       sync.Mutex
	maxDelay time.Duration
	err      error
}

// Chaos Controls

// SetError sets the error to return on calls to the Runner
func (r *ChaosRunner) SetError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

// SetDelay sets the max delay to delay; the actual delay will be randomly selected up to delay
func (r *ChaosRunner) SetDelay(delay time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.maxDelay = delay
}

// Chaos implementations
func (r *ChaosRunner) delay() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.maxDelay != 0 {
		time.Sleep(time.Duration(rand.Int63n(int64(r.maxDelay))))
	}
	return r.err
}

// Implement Runner
func (r *ChaosRunner) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
	err := r.delay()
	if err != nil {
		return runner.ProcessStatus{}, err
	}
	return r.delC.Run(cmd)
}

func (r *ChaosRunner) Status(run runner.RunId) (runner.ProcessStatus, error) {
	err := r.delay()
	if err != nil {
		return runner.ProcessStatus{}, err
	}
	return r.delSts.Status(run)
}

func (r *ChaosRunner) StatusAll() ([]runner.ProcessStatus, error) {
	err := r.delay()
	if err != nil {
		return nil, err
	}
	return r.delSts.StatusAll()
}

func (r *ChaosRunner) Abort(run runner.RunId) (runner.ProcessStatus, error) {
	err := r.delay()
	if err != nil {
		return runner.ProcessStatus{}, err
	}
	return r.delC.Abort(run)
}

func (r *ChaosRunner) Erase(run runner.RunId) error {
	err := r.delay()
	if err != nil {
		return err
	}
	return r.delSts.Erase(run)
}
