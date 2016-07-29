package workerapi

// We should use go generate to run:
//   thrift --gen go:package_prefix=github.com/scootdev/scoot/workerapi/gen-go/,thrift_import=github.com/apache/thrift/lib/go/thrift worker.thrift

import (
	"time"

	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/workerapi/gen-go/worker"
)

//
// Translation between local domain objects and thrift objects:
//

func ThriftRunCommandToDomain(thrift *worker.RunCommand) *runner.Command {
	var timeout time.Duration
	if thrift.TimeoutMs != nil {
		timeout = time.Millisecond * time.Duration(*thrift.TimeoutMs)
	}
	return runner.NewCommand(thrift.Argv, thrift.Env, timeout)
}

func DomainRunCommandToThrift(domain *runner.Command) *worker.RunCommand {
	thrift := worker.NewRunCommand()
	timeoutMs := int32(domain.Timeout / time.Millisecond)
	thrift.TimeoutMs = &timeoutMs
	thrift.Env = domain.EnvVars
	thrift.Argv = domain.Argv
	return thrift
}

func ThriftRunStatusToDomain(thrift *worker.RunStatus) *runner.ProcessStatus {
	domain := &runner.ProcessStatus{}
	domain.RunId = runner.RunId(thrift.RunId)
	switch thrift.Status {
	case worker.Status_UNKNOWN:
		domain.State = runner.UNKNOWN
	case worker.Status_PENDING:
		domain.State = runner.PENDING
	case worker.Status_RUNNING:
		domain.State = runner.RUNNING
	case worker.Status_FAILED:
		domain.State = runner.FAILED
	case worker.Status_ABORTED:
		domain.State = runner.ABORTED
	case worker.Status_TIMEDOUT:
		domain.State = runner.TIMEDOUT
	}
	if thrift.OutUri != nil {
		domain.StdoutRef = *thrift.OutUri
	}
	if thrift.ErrUri != nil {
		domain.StderrRef = *thrift.ErrUri
	}
	if thrift.Error != nil {
		domain.Error = *thrift.Error
	}
	if thrift.ExitCode != nil {
		domain.ExitCode = int(*thrift.ExitCode)
	}
	return domain
}

func DomainRunStatusToThrift(domain *runner.ProcessStatus) *worker.RunStatus {
	thrift := worker.NewRunStatus()
	thrift.RunId = string(domain.RunId)
	switch domain.State {
	case runner.UNKNOWN:
		thrift.Status = worker.Status_UNKNOWN
	case runner.PENDING:
		thrift.Status = worker.Status_PENDING
	case runner.RUNNING:
		thrift.Status = worker.Status_RUNNING
	case runner.FAILED:
		thrift.Status = worker.Status_FAILED
	case runner.ABORTED:
		thrift.Status = worker.Status_ABORTED
	case runner.TIMEDOUT:
		thrift.Status = worker.Status_TIMEDOUT
	}
	thrift.OutUri = &domain.StdoutRef
	thrift.ErrUri = &domain.StderrRef
	thrift.Error = &domain.Error
	exitCode := int32(domain.ExitCode)
	thrift.ExitCode = &exitCode
	return thrift
}
