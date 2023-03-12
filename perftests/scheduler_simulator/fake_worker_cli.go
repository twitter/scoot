package scheduler_simulator

import (
	"fmt"
	"strconv"
	"time"

	"github.com/wisechengyi/scoot/cloud/cluster"
	"github.com/wisechengyi/scoot/common/errors"
	"github.com/wisechengyi/scoot/common/log/tags"
	"github.com/wisechengyi/scoot/runner"
)

/*
FakeWorker sleeps for the amount of time defined in the command's argv[1] entry then
returns exit code defined in argv[2]
*/
type FakeWorker struct {
	node   cluster.Node
	doneCh chan bool
	state  runner.RunState
	cmdId  string
}

func makeFakeWorker(n cluster.Node) *FakeWorker {
	return &FakeWorker{
		node:   n,
		doneCh: make(chan bool),
		state:  runner.PENDING,
	}
}

func (fw *FakeWorker) Run(cmd *runner.Command) (runner.RunStatus, error) {
	fw.cmdId = cmd.TaskID
	if len(cmd.Argv) != 3 {
		return runner.RunStatus{
			RunID:      runner.RunID(fw.cmdId),
			State:      fw.state,
			LogTags:    tags.LogTags{},
			StdoutRef:  "",
			StderrRef:  "",
			SnapshotID: "",
			ExitCode:   1,
			Error:      "",
		}, fmt.Errorf("expected cmd's Argv to have 3 entries: <anything>, <task duration>")
	}
	duration, err := strconv.Atoi(cmd.Argv[1])
	if err != nil {
		return runner.RunStatus{
			RunID:      runner.RunID(fw.cmdId),
			State:      fw.state,
			LogTags:    tags.LogTags{},
			StdoutRef:  "",
			StderrRef:  "",
			SnapshotID: "",
			ExitCode:   1,
			Error:      "",
		}, fmt.Errorf("didn't get a valid (int) task duration value from cmd.Argv's first param:%s", cmd.Argv[1])
	}
	exitCode, err := strconv.Atoi(cmd.Argv[2])
	if err != nil {
		return runner.RunStatus{
			RunID:      runner.RunID(fw.cmdId),
			State:      fw.state,
			LogTags:    tags.LogTags{},
			StdoutRef:  "",
			StderrRef:  "",
			SnapshotID: "",
			ExitCode:   1,
			Error:      "",
		}, fmt.Errorf("didn't get a valid (int) exit code value from cmd.Argv's first param:%s", cmd.Argv[2])
	}

	fw.state = runner.RUNNING
	go func(fw *FakeWorker) {
		t := time.NewTicker(time.Duration(duration) * time.Second)
		<-t.C
		t.Stop()
		fw.doneCh <- true
	}(fw)

	<-fw.doneCh // pause till done
	fw.state = runner.COMPLETE
	rs := runner.RunStatus{
		RunID:      runner.RunID(fw.cmdId),
		State:      fw.state,
		LogTags:    tags.LogTags{},
		StdoutRef:  "",
		StderrRef:  "",
		SnapshotID: "",
		ExitCode:   errors.ExitCode(exitCode),
		Error:      "",
	}
	return rs, nil
}

func (fw *FakeWorker) Abort(run runner.RunID) (runner.RunStatus, error) {
	fw.doneCh <- true
	rs := runner.RunStatus{
		RunID:      runner.RunID(fw.cmdId),
		State:      runner.ABORTED,
		LogTags:    tags.LogTags{},
		StdoutRef:  "",
		StderrRef:  "",
		SnapshotID: "",
		ExitCode:   0,
		Error:      "",
	}
	return rs, nil
}

func (fw *FakeWorker) Release() {}

func (fw *FakeWorker) Query(q runner.Query, w runner.Wait) ([]runner.RunStatus, runner.ServiceStatus, error) {
	return make([]runner.RunStatus, 0), runner.ServiceStatus{Initialized: false, Error: nil}, nil
}

func (fw *FakeWorker) QueryNow(q runner.Query) ([]runner.RunStatus, runner.ServiceStatus, error) {
	return nil, runner.ServiceStatus{Initialized: false, Error: nil}, nil
}

func (fw *FakeWorker) Status(run runner.RunID) (runner.RunStatus, runner.ServiceStatus, error) {
	rs := runner.RunStatus{
		RunID:      runner.RunID(fw.cmdId),
		State:      fw.state,
		LogTags:    tags.LogTags{},
		StdoutRef:  "",
		StderrRef:  "",
		SnapshotID: "",
		ExitCode:   0,
		Error:      "",
	}

	return rs, runner.ServiceStatus{Initialized: false, Error: nil}, nil
}

func (fw *FakeWorker) StatusAll() ([]runner.RunStatus, runner.ServiceStatus, error) {
	return nil, runner.ServiceStatus{Initialized: false, Error: nil}, nil
}
