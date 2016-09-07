package sched

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/sched/gen-go/schedthrift"
	"time"
)

type JobSerializer struct {
	writer thrift.TSerializer
	reader thrift.TDeserializer
}

// if add new XXX serialization formats add XXXSerializer here and add getXXXSerializer() func below
var JsonSerializer JobSerializer = getJsonSerializer()
var BinarySerializer JobSerializer = getBinarySerializer()

func DeserializeJobDef(serializedVal []byte, deserializer JobSerializer) (*Job, error) {

	// create the thrift jobDef struct for the saga log
	internalJobDef := schedthrift.NewJob()

	//NOTE:  must flush here otherwise the deserialization will deserialize the []byte input from the prior call
	deserializer.reader.Transport.Flush()

	// parse the byte string into the thrift jobDef struct
	if err := deserializer.reader.Read(internalJobDef, serializedVal); err != nil {
		return nil, err
	}

	// make a sched JobDefintion from the thrift JobDefinition for the saga log
	schedJob := makeSchedJob(*internalJobDef)

	return &schedJob, nil
}

func makeSchedJob(internalJobDef schedthrift.Job) Job {

	var schedJob = Job{}
	schedJob.Id = internalJobDef.ID
	sagaLogJobDef := internalJobDef.JobDefinition

	var schedJobDef = JobDefinition{}
	schedJobDef.JobType = sagaLogJobDef.JobType
	schedJobDef.Tasks = make(map[string]TaskDefinition)
	for taskName, sagaLogTaskDefTask := range sagaLogJobDef.Tasks {
		argvs := (*(*sagaLogTaskDefTask).Command).Argv
		envVars := (*(*sagaLogTaskDefTask).Command).EnvVars
		timeout, _ := time.ParseDuration(fmt.Sprintf("%dns", (*(*sagaLogTaskDefTask).Command).Timeout))
		snapshotId := (*(*sagaLogTaskDefTask).Command).SnapshotId
		command := runner.NewCommand(argvs, envVars, timeout, snapshotId)
		schedJobDef.Tasks[taskName] = TaskDefinition{*command}
	}
	schedJob.Def = schedJobDef
	return schedJob
}

// NOTE: leaving this method in this file so it lives close to the Deserializer
func (job *Job) SerializeJobDef(serializer JobSerializer) ([]byte, error) {

	// allocate a thrift Job for saga log
	internalJob := schedthrift.NewJob()

	// copy the sched JobDefinition properties to the thrift jobDefinition structure
	(*internalJob).ID = (*job).Id

	internalJobDef := schedthrift.NewJobDefinition()

	jobDef := (*job).Def
	internalJobDef.JobType = jobDef.JobType
	internalJobDef.Tasks = make(map[string]*schedthrift.TaskDefinition)
	for taskName, taskDef := range jobDef.Tasks {
		internalTaskDef := schedthrift.NewTaskDefinition()
		(*internalTaskDef).Command = schedthrift.NewCommand()
		(*(*internalTaskDef).Command).Argv = taskDef.Argv
		(*(*internalTaskDef).Command).EnvVars = taskDef.EnvVars
		(*(*internalTaskDef).Command).SnapshotId = taskDef.SnapshotId
		(*(*internalTaskDef).Command).Timeout = taskDef.Timeout.Nanoseconds()
		internalJobDef.Tasks[taskName] = internalTaskDef
	}
	(*internalJob).JobDefinition = internalJobDef

	if serializedVal, err := serializer.writer.Write(internalJob); err != nil {
		return nil, err
	} else {
		return serializedVal, nil
	}

}

func getJsonSerializer() JobSerializer {

	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTJSONProtocol(transport)
	writer := thrift.TSerializer{Transport: transport, Protocol: protocol}

	reader := thrift.TDeserializer{Transport: transport, Protocol: protocol}
	jobSerializer := JobSerializer{writer, reader}

	return jobSerializer
}

func getBinarySerializer() JobSerializer {

	writer := thrift.NewTSerializer()
	reader := thrift.NewTDeserializer()
	jobSerializer := JobSerializer{*writer, *reader}

	return jobSerializer

}
