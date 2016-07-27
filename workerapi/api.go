package workerapi

// We should use go generate to run:
//   thrift --gen go:package_prefix=github.com/scootdev/scoot/scootapi/gen-go/,thrift_import=github.com/apache/thrift/lib/go/thrift worker.thrift

import (
	"github.com/scootdev/scoot/workerapi/gen-go/worker"
)

//
// Translation between local domain objects and thrift objects:
//

type Status int64

const (
	Status_UNKNOWN   Status = 0
	Status_PENDING   Status = 1
	Status_RUNNING   Status = 2
	Status_COMPLETED Status = 3
	Status_ABORTED   Status = 4
	Status_TIMEOUT   Status = 5
	Status_ORPHANED  Status = 6
	Status_INVALID   Status = 7
)

type WorkerStatus struct {
	Running   []string
	Ended     []string
	VersionId string
}

type RunStatus struct {
	Status   Status
	RunId    string
	OutUri   string
	ErrUri   string
	Info     string
	ExitCode int
}

type RunCommand struct {
	Argv       []string
	SnapshotId string
	TimeoutMs  int
	LeaseMs    int
}

func NewWorkerStatus() *WorkerStatus {
	ws := WorkerStatus{}
	ws.Running = make([]string, 0)
	ws.Ended = make([]string, 0)
	return &ws
}

func NewRunStatus() *RunStatus {
	return &RunStatus{}
}

func NewRunCommand() *RunCommand {
	return &RunCommand{}
}

func ThriftStatusToDomain(thriftStatus worker.Status) Status {
	return Status(thriftStatus)
}

func DomainStatusToThrift(domainStatus Status) worker.Status {
	return worker.Status(domainStatus)
}

func ThriftWorkerStatusToDomain(thriftWStatus *worker.WorkerStatus) *WorkerStatus {
	ws := NewWorkerStatus()
	for _, id := range thriftWStatus.Running {
		ws.Running = append(ws.Running, id)
	}
	for _, id := range thriftWStatus.Ended {
		ws.Ended = append(ws.Ended, id)
	}
	ws.VersionId = *thriftWStatus.VersionId
	return ws
}

func DomainWorkerStatusToThrift(domainWStatus *WorkerStatus) *worker.WorkerStatus {
	ws := worker.NewWorkerStatus()
	ws.Running = make([]string, 0)
	ws.Ended = make([]string, 0)
	for _, id := range domainWStatus.Running {
		ws.Running = append(ws.Running, id)
	}
	for _, id := range domainWStatus.Ended {
		ws.Ended = append(ws.Ended, id)
	}
	ws.VersionId = &domainWStatus.VersionId
	return ws
}

func ThriftRunStatusToDomain(thriftRStatus *worker.RunStatus) *RunStatus {
	rs := NewRunStatus()
	rs.Status = ThriftStatusToDomain(thriftRStatus.Status)
	rs.RunId = *thriftRStatus.RunId
	rs.OutUri = *thriftRStatus.OutUri
	rs.ErrUri = *thriftRStatus.ErrUri
	rs.Info = *thriftRStatus.Info
	rs.ExitCode = int(*thriftRStatus.ExitCode)
	return rs
}

func DomainRunStatusToThrift(domainRStatus *RunStatus) *worker.RunStatus {
	rs := worker.NewRunStatus()
	rs.Status = DomainStatusToThrift(domainRStatus.Status)
	rs.RunId = &domainRStatus.RunId
	rs.OutUri = &domainRStatus.OutUri
	rs.ErrUri = &domainRStatus.ErrUri
	rs.Info = &domainRStatus.Info
	exitCode := int32(domainRStatus.ExitCode)
	rs.ExitCode = &exitCode
	return rs
}

func ThriftRunCommandToDomain(thriftRunCmd *worker.RunCommand) *RunCommand {
	rc := NewRunCommand()
	rc.SnapshotId = ""
	rc.TimeoutMs = 0
	rc.LeaseMs = 0
	rc.Argv = thriftRunCmd.Argv
	if thriftRunCmd.SnapshotId != nil {
		rc.SnapshotId = *thriftRunCmd.SnapshotId
	}
	if thriftRunCmd.TimeoutMs != nil {
		rc.TimeoutMs = int(*thriftRunCmd.TimeoutMs)
	}
	if thriftRunCmd.LeaseMs != nil {
		rc.LeaseMs = int(*thriftRunCmd.LeaseMs)
	}
	return rc
}

func DomainRunCommandToThrift(domainRunCmd *RunCommand) *worker.RunCommand {
	rc := worker.NewRunCommand()
	rc.SnapshotId = &domainRunCmd.SnapshotId
	timeoutMs := int32(domainRunCmd.TimeoutMs)
	leaseMs := int32(domainRunCmd.LeaseMs)
	rc.TimeoutMs = &timeoutMs
	rc.LeaseMs = &leaseMs
	return rc
}
