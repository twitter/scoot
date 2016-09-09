package sched

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/sched/gen-go/schedthrift"
	"time"
)

type Serializer interface {
 	Read(msg thrift.TStruct, b []byte) (err error)
	Write(obj thrift.TStruct) (b []byte, err error)
	DeserializerClose()
}

type JobSerializer struct {
	*thrift.TSerializer
	*thrift.TDeserializer
}

func (t JobSerializer) Read(msg thrift.TStruct, b []byte) (err error) {
	return t.TDeserializer.Read(msg, b)
}
func (t JobSerializer) Write(obj thrift.TStruct) (b []byte, err error) {
	return t.TSerializer.Write(obj)
}

func (t JobSerializer) DeserializerClose() {
	t.TDeserializer.Transport.Close()
}

// if add new XXX serialization formats add XXXSerializer here and add getXXXSerializer() func below
var JsonSerializer JobSerializer
var BinarySerializer JobSerializer

func init() {
	JsonSerializer = getJsonSerializer()
	BinarySerializer = getBinarySerializer()
}

func DeserializeJobDef(serializedVal []byte, deserializer Serializer) (*Job, error) {

	// create the thrift jobDef struct for the saga log
	thriftJob := schedthrift.NewJob()

	//NOTE:  must close here otherwise the deserialization will deserialize the []byte input from the prior call
	deserializer.DeserializerClose()

	// parse the byte string into the thrift jobDef struct
	if err := deserializer.Read(thriftJob, serializedVal); err != nil {
		return nil, err
	}

	// make a sched JobDefintion from the thrift JobDefinition for the saga log
	schedJob := makeSchedJobFromThriftJob(thriftJob)

	return schedJob, nil
}

func makeSchedJobFromThriftJob(thriftJob *schedthrift.Job) *Job {

	var schedJob = Job{}
	schedJob.Id = thriftJob.ID
	sagaLogJobDef := thriftJob.JobDefinition

	var schedJobDef = JobDefinition{}
	schedJobDef.JobType = sagaLogJobDef.JobType
	schedJobDef.Tasks = make(map[string]TaskDefinition)
	for taskName, sagaLogTaskDefTask := range sagaLogJobDef.Tasks {
		var argvs []string
		argvs = sagaLogTaskDefTask.Command.Argv
		var envVars map[string]string
		envVars = sagaLogTaskDefTask.Command.EnvVars
		timeout, _ := time.ParseDuration(fmt.Sprintf("%dns", sagaLogTaskDefTask.Command.Timeout))
		snapshotId := sagaLogTaskDefTask.Command.SnapshotId
		command := runner.NewCommand(argvs, envVars, timeout, snapshotId)
		schedJobDef.Tasks[taskName] = TaskDefinition{*command}
	}
	schedJob.Def = schedJobDef
	return &schedJob
}

func SerializeJobDef(job *Job, serializer Serializer) ([]byte, error) {

	if job == nil {
		return nil, nil
	}

	// allocate a thrift Job for saga log
	internalJob := schedthrift.NewJob()

	// copy the sched JobDefinition properties to the thrift jobDefinition structure
	(*internalJob).ID = (*job).Id

	internalJobDef := schedthrift.NewJobDefinition()

	jobDef := job.Def
	internalJobDef.JobType = jobDef.JobType
	internalJobDef.Tasks = make(map[string]*schedthrift.TaskDefinition)
	for taskName, taskDef := range jobDef.Tasks {
		internalTaskDef := schedthrift.NewTaskDefinition()
		internalTaskDef.Command = schedthrift.NewCommand()
		internalTaskDef.Command.Argv = taskDef.Argv
		internalTaskDef.Command.EnvVars = taskDef.EnvVars
		internalTaskDef.Command.SnapshotId = taskDef.SnapshotId
		internalTaskDef.Command.Timeout = taskDef.Timeout.Nanoseconds()
		internalJobDef.Tasks[taskName] = internalTaskDef
	}
	internalJob.JobDefinition = internalJobDef

	if serializedVal, err := serializer.Write(internalJob); err != nil {
		return nil, err
	} else {
		return serializedVal, nil
	}

}

func getJsonSerializer() JobSerializer {

	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTJSONProtocol(transport)
	writer := &thrift.TSerializer{Transport: transport, Protocol: protocol}

	reader := &thrift.TDeserializer{Transport: transport, Protocol: protocol}
	jobSerializer := JobSerializer{writer, reader}

	return jobSerializer
}

func getBinarySerializer() JobSerializer {

	writer := thrift.NewTSerializer()
	reader := thrift.NewTDeserializer()
	jobSerializer := JobSerializer{writer, reader}

	return jobSerializer

}
