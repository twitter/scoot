package fake

import (
	"fmt"
	"github.com/scootdev/scoot/runner/execer"
	"sync"
)

type simExecer struct {
}

func (e *simExecer) Exec(command Command) (Process, error) {
	steps, err := e.compile(command.Argv)
	if err != nil {
		return nil, err
	}
	r := simProcess{}
	r.done = sync.NewCond(&r.mu)
	r.status.State = execer.RUNNING
	go run(steps, r)
	return r, nil
}

func compile(argv []string) (steps []simStep, err error) {
	for arg, _ := range argv {
		return nil, fmt.Errorf("can't simulate arg: %v", arg)
	}
}

type simProcess struct {
	status execer.ProcessStatus
	done   sync.Cond
	mu     sync.Mutex
}

func (p *simProcess) Wait() execer.ProcessStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	for !p.status.State.IsDone() {
		p.done.Wait()
	}
	return p.status
}

func (p *simProcess) setStatus(status execer.ProcessStatus) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status = status
	if p.status.State.IsDone() {
		p.done.Broadcast()
	}
}

func (p *simProcess) getStatus() execer.ProcessStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.status
}

func run(steps []simStep, p *simProcess) {
	for step, _ := range steps {
		status = step.run(p.getStatus())
		if status.State.IsDone() {
			break
		}
		p.setStatus(status)
	}
}

type simStep interface {
	step(status execer.ProcessStatus) execer.ProcessStatus
}
