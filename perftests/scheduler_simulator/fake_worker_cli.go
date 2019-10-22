package scheduler_simulator

import (
	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/runner"
	"strconv"
	"time"
)

/*
FakeWorker sleeps for the amount of time defined in the command's argv[1] entry then
returns exit code defined in argv[2]
 */
type FakeWorker struct {
	node cluster.Node
	doneCh chan bool
	state runner.RunState
	cmdId  string
}

func makeFakeWorker(n cluster.Node) *FakeWorker {
	return &FakeWorker{
		node: n,
		doneCh: make(chan bool),
		state: runner.PENDING,
	}
}

func (fw *FakeWorker) Run(cmd *runner.Command) (runner.RunStatus, error) {
	fw.cmdId = cmd.TaskID
	duration, _ := strconv.Atoi(cmd.Argv[1])
	fw.state = runner.RUNNING
	go func(fw *FakeWorker) {
		time.Sleep(time.Duration(duration) * time.Second)
		fw.doneCh <- true
	} (fw)

	<- fw.doneCh  // pause till done
	exitCode, _ := strconv.Atoi(cmd.Argv[2])
	fw.state = runner.COMPLETE
	rs := runner.RunStatus{
		RunID:        runner.RunID(fw.cmdId),
		State:        fw.state,
		LogTags:      tags.LogTags{},
		StdoutRef:    "",
		StderrRef:    "",
		SnapshotID:   "",
		ExitCode:     exitCode,
		Error:        "",
		ActionResult: nil,
	}
	return rs, nil
}

func (fw *FakeWorker) Abort(run runner.RunID) (runner.RunStatus, error) {
	fw.doneCh <- true
	rs := runner.RunStatus{
		RunID:        runner.RunID(fw.cmdId),
		State:        runner.ABORTED,
		LogTags:      tags.LogTags{},
		StdoutRef:    "",
		StderrRef:    "",
		SnapshotID:   "",
		ExitCode:     0,
		Error:        "",
		ActionResult: nil,
	}
	return rs, nil
}

func (fw *FakeWorker) Release() {}

func (fw *FakeWorker) Query(q runner.Query, w runner.Wait) ([]runner.RunStatus, runner.ServiceStatus, error) {
	return make([]runner.RunStatus, 0), runner.ServiceStatus{Initialized: false, Error: nil,}, nil
}

func (fw *FakeWorker) QueryNow(q runner.Query) ([]runner.RunStatus, runner.ServiceStatus, error) {
	return nil, runner.ServiceStatus{Initialized: false, Error: nil}, nil
}

func (fw *FakeWorker) Status(run runner.RunID) (runner.RunStatus, runner.ServiceStatus, error) {
	rs := runner.RunStatus{
		RunID:        runner.RunID(fw.cmdId),
		State:        fw.state,
		LogTags:      tags.LogTags{},
		StdoutRef:    "",
		StderrRef:    "",
		SnapshotID:   "",
		ExitCode:     0,
		Error:        "",
		ActionResult: nil,
	}

	return rs, runner.ServiceStatus{Initialized: false, Error: nil}, nil
}

func (fw *FakeWorker) StatusAll() ([]runner.RunStatus, runner.ServiceStatus, error) {
	return nil, runner.ServiceStatus{Initialized: false, Error: nil}, nil
}

func (fw *FakeWorker) Erase(run runner.RunID) error {return nil}
