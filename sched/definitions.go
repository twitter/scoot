// Package sched provides definitions for Scoot Jobs and Tasks
package sched

import (
	"github.com/scootdev/scoot/common/thrifthelpers"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/sched/gen-go/schedthrift"
)

// Job is the job Scoot can schedule
type Job struct {
	Id  string
	Def JobDefinition
}

func (j *Job) Serialize() ([]byte, error) {
	thriftJob, err := makeThriftJobFromDomainJob(j)
	if err != nil {
		return nil, err
	}
	return thrifthelpers.BinarySerialize(thriftJob)
}

func DeserializeJob(input []byte) (*Job, error) {
	var thriftJob *schedthrift.Job
	err := thrifthelpers.BinaryDeserialize(thriftJob, input)

	if err != nil {
		return nil, err
	}

	job := makeDomainJobFromThriftJob(thriftJob)
	return job, nil
}

// JobDefinition is the definition the client sent us
type JobDefinition struct {
	JobType string
	Tasks   map[string]TaskDefinition
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
