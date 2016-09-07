package execers

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/scootdev/scoot/runner/execer"
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
// sleep <millis int>
//   sleep for millis milliseconds
func (e *simExecer) Exec(command execer.Command) (execer.Process, error) {
	steps, err := e.parse(command.Argv)
	if err != nil {
		return nil, err
	}
	r := &simProcess{stdout: command.Stdout, stderr: command.Stderr}
	r.done = sync.NewCond(&r.mu)
	r.status.State = execer.RUNNING
	go r.run(steps)
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
	if strings.HasPrefix(arg, "#") {
		return &noopStep{}, nil
	}
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
	case "sleep":
		i, err := strconv.Atoi(rest)
		if err != nil {
			return nil, err
		}
		return &sleepStep{time.Duration(i) * time.Millisecond}, nil
	case "stdout":
		return &stdoutStep{rest}, nil
	case "stderr":
		return &stderrStep{rest}, nil
	}
	return nil, fmt.Errorf("can't simulate arg: %v", arg)
}

type simProcess struct {
	status execer.ProcessStatus
	done   *sync.Cond
	mu     sync.Mutex

	stdout io.Writer
	stderr io.Writer
}

func (p *simProcess) Wait() execer.ProcessStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	for !p.status.State.IsDone() {
		p.done.Wait()
	}
	return p.status
}

func (p *simProcess) Abort() execer.ProcessStatus {
	st := execer.ProcessStatus{State: execer.FAILED}
	p.setStatus(st)
	return st
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

func (p *simProcess) run(steps []simStep) {
	for _, step := range steps {
		status := p.getStatus()
		if status.State.IsDone() {
			break
		}
		p.setStatus(step.run(status, p))
	}
}

type simStep interface {
	run(status execer.ProcessStatus, p *simProcess) execer.ProcessStatus
}

type completeStep struct {
	exitCode int
}

func (s *completeStep) run(status execer.ProcessStatus, p *simProcess) execer.ProcessStatus {
	status.ExitCode = s.exitCode
	status.State = execer.COMPLETE
	return status
}

type pauseStep struct {
	wait *sync.WaitGroup
}

func (s *pauseStep) run(status execer.ProcessStatus, p *simProcess) execer.ProcessStatus {
	s.wait.Wait()
	return status
}

type sleepStep struct {
	duration time.Duration
}

func (s *sleepStep) run(status execer.ProcessStatus, p *simProcess) execer.ProcessStatus {
	time.Sleep(s.duration)
	return status
}

type stdoutStep struct {
	output string
}

func (s *stdoutStep) run(status execer.ProcessStatus, p *simProcess) execer.ProcessStatus {
	p.stdout.Write([]byte(s.output))
	return status
}

type stderrStep struct {
	output string
}

func (s *stderrStep) run(status execer.ProcessStatus, p *simProcess) execer.ProcessStatus {
	p.stderr.Write([]byte(s.output))
	return status
}

type noopStep struct{}

func (s *noopStep) run(status execer.ProcessStatus, p *simProcess) execer.ProcessStatus {
	return status
}
