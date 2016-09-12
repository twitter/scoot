
package sched

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/sched/gen-go/schedthrift"
	"time"
)

type Serializer interface {
	Deserialize(targetStruct thrift.TStruct, sourceBytes []byte) (err error)
	Serialize(sourceStruct thrift.TStruct) (b []byte, err error)
}

type jsonSerializer struct {}
type binarySerializer struct {}


// clients will provide either the json or binary serializer to the SerializeJob/DeserializeJob operations
var JsonSerializer Serializer = jsonSerializer{}
var BinarySerializer Serializer = binarySerializer{}


// Deserialize a byte slice into a Job object.
func DeserializeJob(serializedVal []byte, deserializer Serializer) (*Job, error) {

	// create the thrift jobDef struct for the saga log
	thriftJob := schedthrift.NewJob()

	// parse the byte string into the thrift jobDef struct
	if err := deserializer.Deserialize(thriftJob, serializedVal); err != nil {
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
		timeout := time.Duration(sagaLogTaskDefTask.Command.Timeout)
		snapshotId := sagaLogTaskDefTask.Command.SnapshotId
		command := runner.NewCommand(argvs, envVars, timeout, snapshotId)
		schedJobDef.Tasks[taskName] = TaskDefinition{*command}
	}
	schedJob.Def = schedJobDef
	return &schedJob
}

// Serialize a Job object to bytes.  Note: nil taks definition maps, envVar maps or
// args slices will serialize to an empty map/slice.
func SerializeJob(job *Job, serializer Serializer) ([]byte, error) {

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

	if serializedVal, err := serializer.Serialize(internalJob); err != nil {
		return nil, err
	} else {
		return serializedVal, nil
	}

}

// Json behavior
func (s jsonSerializer) Deserialize(targetStruct thrift.TStruct, sourceBytes []byte) (err error) {
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTJSONProtocol(transport)

	d := &thrift.TDeserializer{Transport: transport, Protocol: protocol}
	err = d.Read(targetStruct, sourceBytes)
	return err
}

func (s jsonSerializer) Serialize(sourceStruct thrift.TStruct) (b []byte, err error) {
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTJSONProtocol(transport)

	d := &thrift.TSerializer{Transport: transport, Protocol: protocol}
	serializedValue, err := d.Write(sourceStruct)
	return serializedValue, err
}


// Binary behavior
func (s binarySerializer) Deserialize(targetStruct thrift.TStruct, sourceBytes []byte) (err error) {
	d := thrift.NewTDeserializer()
	err = d.Read(targetStruct, sourceBytes)
	return err
}

func (s binarySerializer) Serialize(sourceStruct thrift.TStruct) (b []byte, err error) {
	d := thrift.NewTSerializer()
	serializedValue, err := d.Write(sourceStruct)
	return serializedValue, err
}


