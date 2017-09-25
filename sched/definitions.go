// Package sched provides definitions for Scoot Jobs and Tasks
package sched

import (
	"time"

	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/common/thrifthelpers"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/sched/gen-go/schedthrift"
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

// Task is one task to run
type TaskDefinition struct {
	runner.Command
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

	// Run asap with at least MinRunningNodesForGivenJob, killing youngest lower priority tasks if no nodes are free
	P2

	// Run immediately trying to acquire as many nodes as required, killing all tasks with lower priority that
	// have been executing with duration < 10min or so
	P3
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
