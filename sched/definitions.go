// Package sched provides definitions for Scoot Jobs and Tasks
package sched

import (
	"time"

	"github.com/scootdev/scoot/common/thrifthelpers"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/sched/gen-go/schedthrift"
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
	Tag       string
	Basis     string
	Priority  int32
	Tasks     []TaskDefinition
}

// Task is one task to run
type TaskDefinition struct {
	runner.Command
	TaskID string
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

// transforms a thrift Job into a scheduler Job
func makeDomainJobFromThriftJob(thriftJob *schedthrift.Job) *Job {
	if thriftJob == nil {
		return nil
	}

	thriftJobDef := thriftJob.GetJobDefinition()

	domainTasks := make([]TaskDefinition, 0)
	if thriftJobDef != nil {
		for _, task := range thriftJobDef.GetTasks() {
			cmd := task.GetCommand()

			command := runner.Command{
				Argv:       cmd.GetArgv(),
				EnvVars:    cmd.GetEnvVars(),
				Timeout:    time.Duration(cmd.GetTimeout()),
				SnapshotID: cmd.GetSnapshotId(),
				JobID:      task.GetJobId(),
				TaskID:     task.GetTaskId(),
			}
			domainTasks = append(domainTasks, TaskDefinition{command, task.GetTaskId()})
		}
	}

	jobType := ""
	if thriftJobDef != nil {
		jobType = thriftJobDef.GetJobType()
	}

	priority := int32(0)
	if thriftJobDef != nil {
		priority = thriftJobDef.GetPriority()
	}

	tag := ""
	if thriftJobDef != nil {
		tag = thriftJobDef.GetTag()
	}

	basis := ""
	if thriftJobDef != nil {
		basis = thriftJobDef.GetBasis()
	}

	requestor := ""
	if thriftJobDef != nil {
		requestor = thriftJobDef.GetRequestor()
	}

	domainJobDef := JobDefinition{
		JobType:   jobType,
		Tasks:     domainTasks,
		Priority:  priority,
		Tag:       tag,
		Basis:     basis,
		Requestor: requestor,
	}

	return &Job{
		Id:  thriftJob.GetID(),
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
		jobId := domainJob.Id
		taskId := domainTask.TaskID
		thriftTask := schedthrift.TaskDefinition{Command: &cmd, JobId: &jobId, TaskId: &taskId}
		thriftTasks = append(thriftTasks, &thriftTask)
	}

	thriftJobDefinition := schedthrift.JobDefinition{
		JobType:   &(*domainJob).Def.JobType,
		Tasks:     thriftTasks,
		Priority:  &(*domainJob).Def.Priority,
		Tag:       &(*domainJob).Def.Tag,
		Basis:     &(*domainJob).Def.Basis,
		Requestor: &(*domainJob).Def.Requestor,
	}

	thriftJob := schedthrift.Job{
		ID:            domainJob.Id,
		JobDefinition: &thriftJobDefinition,
	}

	return &thriftJob, nil

}
