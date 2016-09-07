package sched

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/sched/gen-go/sagalog"
	"time"
)

const (
	JsonSerialize   string = "json"
	BinarySerialize string = "binary"
)

func Deserialize(serializedVal []byte, serializeType string) (*Job, error) {

	// create the thrift deserializer
	deserializer, err := getDeserializer(serializeType)
	if err != nil {
		return nil, err
	}

	// create the thrift jobDef struct for the saga log
	sagaLogJob := sagalog.NewJob()

	// parse the byte string into the thrift jobDef struct
	if err := deserializer.Read(sagaLogJob, serializedVal); err != nil {
		return nil, err
	}

	// make a sched JobDefintion from the thrift JobDefinition for the saga log
	schedJob := makeSchedJob(*sagaLogJob)

	return &schedJob, nil
}

// note: tried to declare this as a method on sagalog.JobDefintion - couldn't get the syntax right
func makeSchedJob(sagaLogJob sagalog.Job) Job {

	var schedJob = Job{}
	schedJob.Id = sagaLogJob.ID
	sagaLogJobDef := sagaLogJob.JobDefinition

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

func Serialize(job *Job, serializeType string) ([]byte, error) {

	if job == nil {
		return nil, fmt.Errorf("Cannot serialize a nil object")
	}

	serializer, err := getSerializer(serializeType)
	if err != nil {
		return nil, err
	}

	// allocate a thrift Job for saga log
	sagalogJob := sagalog.NewJob()

	// copy the sched JobDefinition properties to the thrift jobDefinition structure
	(*sagalogJob).ID = (*job).Id

	sagalogJobDefinition := sagalog.NewJobDefinition()

	jobDef := (*job).Def
	sagalogJobDefinition.JobType = jobDef.JobType
	sagalogJobDefinition.Tasks = make(map[string]*sagalog.TaskDefinition)
	for taskName, taskDef := range jobDef.Tasks {
		sagaTaskDef := sagalog.NewTaskDefinition()
		(*sagaTaskDef).Command = sagalog.NewCommand()
		(*(*sagaTaskDef).Command).Argv = taskDef.Argv
		(*(*sagaTaskDef).Command).EnvVars = taskDef.EnvVars
		(*(*sagaTaskDef).Command).SnapshotId = taskDef.SnapshotId
		(*(*sagaTaskDef).Command).Timeout = taskDef.Timeout.Nanoseconds()
		sagalogJobDefinition.Tasks[taskName] = sagaTaskDef
	}
	(*sagalogJob).JobDefinition = sagalogJobDefinition

	if serializedVal, err := serializer.Write(sagalogJob); err != nil {
		return nil, err
	} else {
		return serializedVal, nil
	}

}

func getSerializer(serializeType string) (*thrift.TSerializer, error) {

	switch serializeType {
	case JsonSerialize:
		transport := thrift.NewTMemoryBufferLen(1024)
		protocol := thrift.NewTJSONProtocol(transport)
		serializer := thrift.TSerializer{Transport:transport, Protocol:protocol}
		return &serializer, nil
	case BinarySerialize:
		serializer := thrift.NewTSerializer()
		return serializer, nil
	default:
		return nil, fmt.Errorf("Invalid serializer type: %s\n", serializeType)
	}

}

func getDeserializer(serializeType string) (*thrift.TDeserializer, error) {

	switch serializeType {
	case JsonSerialize:
		transport := thrift.NewTMemoryBufferLen(1024)
		protocol := thrift.NewTJSONProtocol(transport)
		deserializer := thrift.TDeserializer{Transport:transport, Protocol:protocol}
		return &deserializer, nil
	case BinarySerialize:
		deserializer := thrift.NewTDeserializer()
		return deserializer, nil
	default:
		return nil, fmt.Errorf("Invalid deserializer type: %s\n", serializeType)
	}
}
