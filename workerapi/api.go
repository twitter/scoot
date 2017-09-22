package workerapi

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/log/helpers"
	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/common/thrifthelpers"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/workerapi/gen-go/worker"
)

//
// Translation between local domain objects and thrift objects:
//

//TODO: test workerStatus.
type WorkerStatus struct {
	Runs        []runner.RunStatus
	Initialized bool
	Error       string
}

func ThriftWorkerStatusToDomain(thrift *worker.WorkerStatus) WorkerStatus {
	runs := make([]runner.RunStatus, 0)
	for _, r := range thrift.Runs {
		runs = append(runs, ThriftRunStatusToDomain(r))
	}
	return WorkerStatus{runs, thrift.Initialized, thrift.Error}
}

func DomainWorkerStatusToThrift(domain WorkerStatus) *worker.WorkerStatus {
	thrift := worker.NewWorkerStatus()
	thrift.Runs = make([]*worker.RunStatus, 0)
	for _, r := range domain.Runs {
		thrift.Runs = append(thrift.Runs, DomainRunStatusToThrift(r))
		thrift.Initialized = domain.Initialized
		thrift.Error = domain.Error
	}
	return thrift
}

func ThriftRunCommandToDomain(thrift *worker.RunCommand) *runner.Command {
	log.Info("ThriftRunCommandToDomain %v", thrift)
	argv := make([]string, 0)
	env := make(map[string]string)
	timeout := time.Duration(0)
	snapshotID := ""
	jobID := ""
	taskID := ""
	tag := ""
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
	if thrift.TaskId != nil {
		taskID = *thrift.TaskId
	}
	if thrift.JobId != nil {
		jobID = *thrift.JobId
	}
	if thrift.Tag != nil {
		tag = *thrift.Tag
	}
	return &runner.Command{
		Argv:       argv,
		EnvVars:    env,
		Timeout:    timeout,
		SnapshotID: snapshotID,
		LogTags: tags.LogTags{
			JobID:  jobID,
			TaskID: taskID,
			Tag:    tag,
		},
	}
}

func DomainRunCommandToThrift(domain *runner.Command) *worker.RunCommand {
	thrift := worker.NewRunCommand()
	timeoutMs := int32(domain.Timeout / time.Millisecond)
	thrift.TimeoutMs = &timeoutMs
	thrift.Env = domain.EnvVars
	thrift.Argv = domain.Argv
	snapID := domain.SnapshotID
	thrift.SnapshotId = &snapID
	jobID := domain.JobID
	thrift.JobId = &jobID
	taskID := domain.TaskID
	thrift.TaskId = &taskID
	tag := domain.Tag
	thrift.Tag = &tag
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
	if thrift.SnapshotId != nil {
		domain.SnapshotID = *thrift.SnapshotId
	}
	if thrift.JobId != nil {
		domain.JobID = *thrift.JobId
	}
	if thrift.TaskId != nil {
		domain.TaskID = *thrift.TaskId
	}
	if thrift.Tag != nil {
		domain.Tag = *thrift.Tag
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
	case runner.PREPARING, runner.RUNNING:
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
	thrift.OutUri = helpers.CopyStringToPointer(domain.StdoutRef)
	thrift.ErrUri = helpers.CopyStringToPointer(domain.StderrRef)
	thrift.Error = helpers.CopyStringToPointer(domain.Error)
	exitCode := int32(domain.ExitCode)
	thrift.ExitCode = &exitCode
	thrift.SnapshotId = helpers.CopyStringToPointer(domain.SnapshotID)
	thrift.JobId = helpers.CopyStringToPointer(domain.JobID)
	thrift.TaskId = helpers.CopyStringToPointer(domain.TaskID)
	thrift.Tag = helpers.CopyStringToPointer(domain.Tag)
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
