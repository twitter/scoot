// Package domain provides definitions for Scoot Jobs and Tasks
package domain

import (
	"fmt"
	"time"

	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/common/thrifthelpers"
	"github.com/twitter/scoot/runner"
	schedthrift "github.com/twitter/scoot/scheduler/domain/gen-go/sched"
)

// Job is the job Scoot can schedule
type Job struct {
	Id  string
	Def JobDefinition
}

// Serialize Job to binary slice, and error is
// returned if the object cannot be Serialized
func (j *Job) Serialize() ([]byte, error) {
	thriftJob, err := makeThriftJobFromDomainJob(j)
	if err != nil {
		return nil, err
	}
	return thrifthelpers.BinarySerialize(thriftJob)
}

// Desrialize a binary slice to a Job,
// an error is returned if it cannot be deserialized.
func DeserializeJob(input []byte) (*Job, error) {
	thriftJob := schedthrift.NewJob()
	err := thrifthelpers.BinaryDeserialize(thriftJob, input)

	if err != nil {
		return nil, err
	}

	job := makeDomainJobFromThriftJob(thriftJob)
	return job, nil
}

// JobDefinition is the definition the client sent us
type JobDefinition struct {
	JobType   string
	Requestor string
	Basis     string
	Tag       string
	Priority  Priority
	Tasks     []TaskDefinition
}

func (jd *JobDefinition) String() string {
	return fmt.Sprintf("jobType:%s, req:%s, tag:%s, basis: %s, tasks:%d", jd.JobType, jd.Requestor, jd.Tag, jd.Basis, len(jd.Tasks))
}

// Task is one task to run
type TaskDefinition struct {
	runner.Command
}

type OfflineWorkerReq struct {
	ID        string
	Requestor string
}

type ReinstateWorkerReq struct {
	ID        string
	Requestor string
}

// Status for Job & Tasks
type Status int

const (
	// NotRunning, waiting to be scheduled
	NotStarted Status = iota

	// Currently Scheduled and In Progress Job/Task
	InProgress

	// Successfully Completed Job/Task
	Completed

	// Job was Aborted, Compensating Tasks are being Applied.
	// A RollingBack task has not finished its compensating
	// tasks yet.
	RollingBack

	// Job/Task finished unsuccessfully all compensating actions
	// have been applied.
	RolledBack
)

func (s Status) String() string {
	asString := [6]string{"NotStarted", "InProgress", "Completed", "Killed", "RollingBack", "RolledBack"}
	return asString[s]
}

type Priority int

const (
	// Default, queue new runs until any resources are available
	P0 Priority = iota

	// Run asap, ahead of priority=0, consuming nodes as they become available if no nodes are free
	P1

	// Run asap, ahead of priority=1, consuming nodes as they become available if no nodes are free
	P2
)

// transforms a thrift Job into a scheduler Job
func makeDomainJobFromThriftJob(thriftJob *schedthrift.Job) *Job {
	if thriftJob == nil {
		return nil
	}
	jobType := ""
	priority := int32(0)
	tag := ""
	basis := ""
	requestor := ""

	thriftJobDef := thriftJob.GetJobDefinition()
	jobID := thriftJob.GetID()

	domainTasks := make([]TaskDefinition, 0)
	if thriftJobDef != nil {
		for _, task := range thriftJobDef.GetTasks() {
			cmd := task.GetCommand()

			command := runner.Command{
				Argv:       cmd.GetArgv(),
				EnvVars:    cmd.GetEnvVars(),
				Timeout:    time.Duration(cmd.GetTimeout()),
				SnapshotID: cmd.GetSnapshotId(),
				LogTags: tags.LogTags{
					JobID:  jobID,
					TaskID: task.GetTaskId(),
					Tag:    thriftJobDef.GetTag(),
				},
			}

			domainTasks = append(domainTasks, TaskDefinition{command})
		}

		jobType = thriftJobDef.GetJobType()
		priority = thriftJobDef.GetPriority()
		tag = thriftJobDef.GetTag()
		basis = thriftJobDef.GetBasis()
		requestor = thriftJobDef.GetRequestor()
	}

	domainJobDef := JobDefinition{
		JobType:   jobType,
		Tasks:     domainTasks,
		Priority:  Priority(priority),
		Basis:     basis,
		Requestor: requestor,
		Tag:       tag,
	}

	return &Job{
		Id:  jobID,
		Def: domainJobDef,
	}
}

// converts a scheduler Job into a Thrift Job
func makeThriftJobFromDomainJob(domainJob *Job) (*schedthrift.Job, error) {
	if domainJob == nil {
		return nil, nil
	}

	thriftTasks := make([]*schedthrift.TaskDefinition, 0)
	for _, domainTask := range domainJob.Def.Tasks {
		to := int64(domainTask.Timeout)
		cmd := schedthrift.Command{
			Argv:       domainTask.Argv,
			EnvVars:    domainTask.EnvVars,
			Timeout:    &to,
			SnapshotId: domainTask.SnapshotID,
		}
		taskId := domainTask.TaskID

		thriftTask := schedthrift.TaskDefinition{Command: &cmd, TaskId: &taskId}
		thriftTasks = append(thriftTasks, &thriftTask)
	}

	prio := int32(domainJob.Def.Priority)
	thriftJobDefinition := schedthrift.JobDefinition{
		JobType:   &(*domainJob).Def.JobType,
		Tasks:     thriftTasks,
		Priority:  &prio,
		Tag:       &(domainJob).Def.Tag,
		Basis:     &(domainJob).Def.Basis,
		Requestor: &(domainJob).Def.Requestor,
	}

	thriftJob := schedthrift.Job{
		ID:            domainJob.Id,
		JobDefinition: &thriftJobDefinition,
	}

	return &thriftJob, nil
}

// Validate a job, returning an *InvalidJobRequest if invalid.
func ValidateJob(job JobDefinition) error {
	if len(job.Tasks) == 0 {
		return fmt.Errorf("invalid job. Must have at least 1 task; was empty")
	}
	for _, task := range job.Tasks {
		if task.TaskID == "" {
			return fmt.Errorf("invalid task id \"\".")
		}
		if len(task.Command.Argv) == 0 {
			return fmt.Errorf("invalid task.Command.Argv. Must have at least one argument; was empty")
		}
	}
	return nil
}

func ValidateMaxTasks(maxTasks int) error {
	// validation is also implemented in scheduler/client/client.go.  This implementation cannot be used
	// there because it causes a circular dependency.  The two implementations can be consolidated
	// when the code is restructured
	if maxTasks < -1 {
		return fmt.Errorf("invalid tasks limit:%d. Must be >= -1.", maxTasks)
	}

	return nil
}
