package protocol

import (
	"github.com/scootdev/scoot/runner"
)

// TODO(dbentley): we should go generate with command protoc daemon.proto --go_out=plugins=grpc:.

func FromRunnerStatus(status runner.ProcessStatus) *ProcessStatus {
	state := ProcessState_UNKNOWN
	switch status.State {
	case runner.UNKNOWN:
		state = ProcessState_UNKNOWN
	case runner.PENDING:
		state = ProcessState_PENDING
	case runner.RUNNING:
		state = ProcessState_RUNNING
	case runner.FAILED, runner.ABORTED, runner.TIMEDOUT:
		state = ProcessState_FAILED
	}
	return &ProcessStatus{
		string(status.RunId),
		state,
		status.StdoutRef,
		status.StderrRef,
		int32(status.ExitCode),
		status.Error,
	}
}

func ToRunnerStatus(status *ProcessStatus) runner.ProcessStatus {
	state := runner.UNKNOWN
	switch status.State {
	case ProcessState_UNKNOWN:
		state = runner.UNKNOWN
	case ProcessState_PENDING:
		state = runner.PENDING
	case ProcessState_RUNNING:
		state = runner.RUNNING
	case ProcessState_FAILED:
		state = runner.FAILED
	}
	return runner.ProcessStatus{
		RunId:     runner.RunId(status.RunId),
		State:     state,
		StdoutRef: status.StdoutRef,
		StderrRef: status.StderrRef,
		ExitCode:  int(status.ExitCode),
		Error:     status.Error,
	}
}
