package fake

import (
	"fmt"
	"github.com/scootdev/scoot/runner/execer"
	"strconv"
	"strings"
	"sync"
)

func NewSimExecer(wait *sync.WaitGroup) execer.Execer {
	return &simExecer{wait}
}

type simExecer struct {
	wait *sync.WaitGroup
}

// simExecer execs by simulating running argv.
// each arg in command.argv is simulated in order.
// valid args are:
// complete <exitcode int>
//   complete with exitcode
// pause
//   pause until the simExecer's WaitGroup is Done
func (e *simExecer) Exec(command execer.Command) (execer.Process, error) {
	steps, err := e.parse(command.Argv)
	if err != nil {
		return nil, err
	}
	r := &simProcess{}
	r.done = sync.NewCond(&r.mu)
	r.status.State = execer.RUNNING
	go run(steps, r)
	return r, nil
}

// parse parses an argv into sim steps
func (e *simExecer) parse(argv []string) (steps []simStep, err error) {
	for _, arg := range argv {
		s, err := e.parseArg(arg)
		if err != nil {
			return nil, err
		}
		steps = append(steps, s)
	}
	return steps, nil
}

func (e *simExecer) parseArg(arg string) (simStep, error) {
	splits := strings.SplitN(arg, " ", 2)
	opcode, rest := splits[0], ""
	if len(splits) == 2 {
		rest = splits[1]
	}
	switch opcode {
	case "complete":
		i, err := strconv.Atoi(rest)
		if err != nil {
			return nil, err
		}
		return &completeStep{i}, nil
	case "pause":
		return &pauseStep{e.wait}, nil
	}
	return nil, fmt.Errorf("can't simulate arg: %v", arg)
}

type simProcess struct {
	status execer.ProcessStatus
	done   *sync.Cond
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
	if p.status.State.IsDone() {
		return
	}
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
	for _, step := range steps {
		status := p.getStatus()
		if status.State.IsDone() {
			break
		}
		p.setStatus(step.run(status))
	}
}

type simStep interface {
	run(status execer.ProcessStatus) execer.ProcessStatus
}

type completeStep struct {
	exitCode int
}

func (s *completeStep) run(status execer.ProcessStatus) execer.ProcessStatus {
	status.ExitCode = s.exitCode
	status.State = execer.COMPLETE
	return status
}

type pauseStep struct {
	wait *sync.WaitGroup
}

func (s *pauseStep) run(status execer.ProcessStatus) execer.ProcessStatus {
	s.wait.Wait()
	return status
}
