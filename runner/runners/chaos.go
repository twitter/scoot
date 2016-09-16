package runners

import (
	"math/rand"
	"time"

	"github.com/scootdev/scoot/runner"
)

// Creates a new Chaos Runner
func NewChaosRunner(delegate runner.Runner, delay time.Duration) *ChaosRunner {
	return &ChaosRunner{delegate, delay, nil}
}

// ChaosRunner implements Runner by calling to a delegate runner in the happy path,
// but delaying a random time between 0 and MaxDelay, or returning an error
type ChaosRunner struct {
	del      runner.Runner
	MaxDelay time.Duration
	Err      error
}

func (r *ChaosRunner) delay() {
	if r.MaxDelay == 0 {
		return
	}
	time.Sleep(time.Duration(rand.Int63n(int64(r.MaxDelay))))
}

func (r *ChaosRunner) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
	r.delay()
	if r.Err != nil {
		return runner.ProcessStatus{}, r.Err
	}
	return r.del.Run(cmd)
}

func (r *ChaosRunner) Status(run runner.RunId) (runner.ProcessStatus, error) {
	r.delay()
	if r.Err != nil {
		return runner.ProcessStatus{}, r.Err
	}
	return r.del.Status(run)
}

func (r *ChaosRunner) StatusAll() ([]runner.ProcessStatus, error) {
	r.delay()
	if r.Err != nil {
		return nil, r.Err
	}
	return r.del.StatusAll()
}

func (r *ChaosRunner) Abort(run runner.RunId) (runner.ProcessStatus, error) {
	r.delay()
	if r.Err != nil {
		return runner.ProcessStatus{}, r.Err
	}
	return r.Abort(run)
}

func (r *ChaosRunner) Erase(run runner.RunId) error {
	r.delay()
	if r.Err != nil {
		return r.Err
	}
	return r.Erase(run)
}
