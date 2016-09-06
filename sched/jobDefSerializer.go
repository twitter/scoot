package sched

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/sched/gen-go/sagalog"
	"github.com/scootdev/scoot/runner"
	"time"
	"fmt"
)

const (
	JsonSerialize string = "json"
	BinarySerialize string = "binary"
)

func Deserialize(serializedVal []byte, serializeType string) (*JobDefinition, error) {

	// create the thrift deserializer
	//deserializer := thrift.NewTDeserializer();
	deserializer, err := getDeserializer(serializeType)
	if (err != nil) {
		return nil, err
	}

	// create the thrift jobDef struct for the saga log
	sagaLogJobDef := sagalog.NewJobDefinition()
	// parse the byte string into the thrift jobDef struct
	if err := deserializer.Read(sagaLogJobDef, serializedVal); err != nil {
		return nil, err
	}

	// make a sched JobDefintion from the thrift JobDefinition for the saga log
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
	return &schedJobDef, nil
}

func Serialize(jobDef *JobDefinition, serializeType string) ([]byte, error) {

	serializer, err := getSerializer(serializeType)
	if (err != nil) {
		return nil, err
	}

	// allocate a thrift JobDefinition for saga log
	sagalogJobDefinition := sagalog.NewJobDefinition()

	// copy the sched JobDefinition properties to the thrift jobDefinition structure
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

	if serializedVal, err := serializer.Write(sagalogJobDefinition); err != nil {
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
		serializer := thrift.TSerializer{transport, protocol}
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
		deserializer := thrift.TDeserializer{transport, protocol}
		return &deserializer, nil
	case BinarySerialize:
		deserializer := thrift.NewTDeserializer()
		return deserializer, nil
	default:
		return nil, fmt.Errorf("Invalid deserializer type: %s\n", serializeType)
	}
}
