package runners

import (
	"math/rand"
	"time"

	"github.com/scootdev/scoot/runner"
)

func NewChaosRunner(delegate runner.Runner, delay time.Duration) *ChaosRunner {
	return &ChaosRunner{delegate, delay, runner.ProcessStatus{}}
}

type ChaosRunner struct {
	del         runner.Runner
	MaxDelay    time.Duration
	ErrorStatus runner.ProcessStatus
}

func (r *ChaosRunner) delay() {
	if r.MaxDelay == 0 {
		return
	}
	time.Sleep(time.Duration(rand.Int63n(int64(r.MaxDelay))))
}

func (r *ChaosRunner) Run(cmd *runner.Command) runner.ProcessStatus {
	r.delay()
	if r.ErrorStatus.State != runner.UNKNOWN {
		return r.ErrorStatus
	}
	return r.del.Run(cmd)
}

func (r *ChaosRunner) Status(run runner.RunId) runner.ProcessStatus {
	r.delay()
	if r.ErrorStatus.State != runner.UNKNOWN {
		return r.ErrorStatus
	}
	return r.del.Status(run)
}

func (r *ChaosRunner) StatusAll() []runner.ProcessStatus {
	r.delay()
	return r.del.StatusAll()
}

func (r *ChaosRunner) Abort(run runner.RunId) runner.ProcessStatus {
	r.delay()
	if r.ErrorStatus.State != runner.UNKNOWN {
		return r.ErrorStatus
	}
	return r.Abort(run)
}

func (r *ChaosRunner) Erase(run runner.RunId) {
	r.delay()
	r.Erase(run)
}
