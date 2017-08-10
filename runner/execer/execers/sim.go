package execers

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twitter/scoot/runner/execer"
)

func NewSimExecer() *SimExecer {
	return &SimExecer{resumeCh: make(chan struct{})}
}

type SimExecer struct {
	resumeCh chan struct{}
}

// SimExecer execs by simulating running argv.
// each arg in command.argv is simulated in order.
// valid args are:
// complete <exitcode int>
//   complete with exitcode
// pause
//   pause until SimExecer.Resume() is called
// sleep <millis int>
//   sleep for millis milliseconds
// stdout <message>
//   put <message> in stdout in the response
// stderr <message>
//   put <message> in stderr in the response
// NB: SimExecer is often hid behind an InterceptExecer, so you should pass
// argv[0] as "#! sim execer" (Cf. intercept.go)
func (e *SimExecer) Exec(command execer.Command) (execer.Process, error) {
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

func (e *SimExecer) Resume() {
	e.resumeCh <- struct{}{}
}

// parse parses an argv into sim steps
func (e *SimExecer) parse(argv []string) (steps []simStep, err error) {
	for _, arg := range argv {
		s, err := e.parseArg(arg)
		if err != nil {
			return nil, err
		}
		steps = append(steps, s)
	}
	return steps, nil
}

func (e *SimExecer) parseArg(arg string) (simStep, error) {
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
			t := fmt.Errorf("error parsing <n> in complete <n>:%s", err.Error())
			return nil, t
		}
		return &completeStep{i}, nil
	case "pause":
		return &pauseStep{e.resumeCh}, nil
	case "sleep":
		i, err := strconv.Atoi(rest)
		if err != nil {
			t := fmt.Errorf("error parsing <n> in sleep <n>:%s", err.Error())
			return nil, t
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
	ch chan struct{}
}

func (s *pauseStep) run(status execer.ProcessStatus, p *simProcess) execer.ProcessStatus {
	abortCh := make(chan struct{})
	go func() {
		// Waits until this process is stopped (by being aborted)
		p.Wait()
		close(abortCh)
	}()
	// wait for the first of being aborted or SimExecer.Resume()
	select {
	case <-abortCh:
	case <-s.ch:
	}
	return status
}

type sleepStep struct {
	duration time.Duration
}

func (s *sleepStep) run(status execer.ProcessStatus, p *simProcess) execer.ProcessStatus {
	time.Sleep(s.duration)
	status.State = execer.COMPLETE
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
