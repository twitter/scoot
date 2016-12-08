package workerapi

import (
	"time"

	"github.com/scootdev/scoot/common/thrifthelpers"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/workerapi/gen-go/worker"
)

//
// Translation between local domain objects and thrift objects:
//

//TODO: test workerStatus.
type WorkerStatus struct {
	Runs []runner.RunStatus
}

func ThriftWorkerStatusToDomain(thrift *worker.WorkerStatus) WorkerStatus {
	runs := make([]runner.RunStatus, 0)
	for _, r := range thrift.Runs {
		runs = append(runs, ThriftRunStatusToDomain(r))
	}
	return WorkerStatus{runs}
}

func DomainWorkerStatusToThrift(domain WorkerStatus) *worker.WorkerStatus {
	thrift := worker.NewWorkerStatus()
	thrift.Runs = make([]*worker.RunStatus, 0)
	for _, r := range domain.Runs {
		thrift.Runs = append(thrift.Runs, DomainRunStatusToThrift(r))
	}
	return thrift
}

func ThriftRunCommandToDomain(thrift *worker.RunCommand) *runner.Command {
	argv := make([]string, 0)
	env := make(map[string]string)
	timeout := time.Duration(0)
	snapshotID := ""
	if thrift.Argv != nil {
		argv = thrift.Argv
	}
	if thrift.Env != nil {
		env = thrift.Env
	}
	if thrift.TimeoutMs != nil {
		timeout = time.Millisecond * time.Duration(*thrift.TimeoutMs)
	}
	if thrift.SnapshotId != nil {
		snapshotID = *thrift.SnapshotId
	}
	return &runner.Command{Argv: argv, EnvVars: env, Timeout: timeout, SnapshotID: snapshotID}
}

func DomainRunCommandToThrift(domain *runner.Command) *worker.RunCommand {
	thrift := worker.NewRunCommand()
	timeoutMs := int32(domain.Timeout / time.Millisecond)
	thrift.TimeoutMs = &timeoutMs
	thrift.Env = domain.EnvVars
	thrift.Argv = domain.Argv
	snapID := domain.SnapshotID
	thrift.SnapshotId = &snapID
	return thrift
}

func ThriftRunStatusToDomain(thrift *worker.RunStatus) runner.RunStatus {
	domain := runner.RunStatus{}
	domain.RunID = runner.RunID(thrift.RunId)
	switch thrift.Status {
	case worker.Status_UNKNOWN:
		domain.State = runner.UNKNOWN
	case worker.Status_PENDING:
		domain.State = runner.PENDING
	case worker.Status_RUNNING:
		domain.State = runner.RUNNING
	case worker.Status_COMPLETE:
		domain.State = runner.COMPLETE
	case worker.Status_FAILED:
		domain.State = runner.FAILED
	case worker.Status_ABORTED:
		domain.State = runner.ABORTED
	case worker.Status_TIMEDOUT:
		domain.State = runner.TIMEDOUT
	case worker.Status_BADREQUEST:
		domain.State = runner.BADREQUEST
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

func DomainRunStatusToThrift(domain runner.RunStatus) *worker.RunStatus {
	thrift := worker.NewRunStatus()
	thrift.RunId = string(domain.RunID)
	switch domain.State {
	case runner.UNKNOWN:
		thrift.Status = worker.Status_UNKNOWN
	case runner.PENDING:
		thrift.Status = worker.Status_PENDING
	case runner.RUNNING:
		thrift.Status = worker.Status_RUNNING
	case runner.COMPLETE:
		thrift.Status = worker.Status_COMPLETE
	case runner.FAILED:
		thrift.Status = worker.Status_FAILED
	case runner.ABORTED:
		thrift.Status = worker.Status_ABORTED
	case runner.TIMEDOUT:
		thrift.Status = worker.Status_TIMEDOUT
	case runner.BADREQUEST:
		thrift.Status = worker.Status_BADREQUEST
	}
	thrift.OutUri = &domain.StdoutRef
	thrift.ErrUri = &domain.StderrRef
	thrift.Error = &domain.Error
	exitCode := int32(domain.ExitCode)
	thrift.ExitCode = &exitCode
	return thrift
}

func SerializeProcessStatus(processStatus runner.RunStatus) ([]byte, error) {

	runStatus := DomainRunStatusToThrift(processStatus)

	asBytes, err := thrifthelpers.JsonSerialize(runStatus)

	if err != nil {
		return nil, err
	}

	return asBytes, err
}
