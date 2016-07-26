package protocol

import (
	"github.com/scootdev/scoot/runner"
)

// TODO(dbentley): we should go generate with command protoc daemon.proto --go_out=plugins=grpc:.

func FromRunnerStatus(status runner.ProcessStatus) *ProcessStatus {
	return &ProcessStatus{
		string(status.RunId),
		ProcessState(status.State),
		status.StdoutRef,
		status.StderrRef,
		int32(status.ExitCode),
		status.Error,
	}
}

func ToRunnerStatus(status *ProcessStatus) runner.ProcessStatus {
	return runner.ProcessStatus{
		RunId:     runner.RunId(status.RunId),
		State:     runner.ProcessState(status.State),
		StdoutRef: status.StdoutRef,
		StderrRef: status.StderrRef,
		ExitCode:  int(status.ExitCode),
		Error:     status.Error,
	}
}
