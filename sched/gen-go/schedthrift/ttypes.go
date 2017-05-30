// Autogenerated by Thrift Compiler (0.9.3)
// DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING

package schedthrift

import (
	"bytes"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
)

// (needed to ensure safety because of naive import list construction.)
var _ = thrift.ZERO
var _ = fmt.Printf
var _ = bytes.Equal

var GoUnusedProtection__ int

// Attributes:
//  - Argv
//  - EnvVars
//  - Timeout
//  - SnapshotId
type Command struct {
	Argv       []string          `thrift:"argv,1,required" json:"argv"`
	EnvVars    map[string]string `thrift:"envVars,2" json:"envVars,omitempty"`
	Timeout    *int64            `thrift:"timeout,3" json:"timeout,omitempty"`
	SnapshotId string            `thrift:"snapshotId,4,required" json:"snapshotId"`
}

func NewCommand() *Command {
	return &Command{}
}

func (p *Command) GetArgv() []string {
	return p.Argv
}

var Command_EnvVars_DEFAULT map[string]string

func (p *Command) GetEnvVars() map[string]string {
	return p.EnvVars
}

var Command_Timeout_DEFAULT int64

func (p *Command) GetTimeout() int64 {
	if !p.IsSetTimeout() {
		return Command_Timeout_DEFAULT
	}
	return *p.Timeout
}

func (p *Command) GetSnapshotId() string {
	return p.SnapshotId
}
func (p *Command) IsSetEnvVars() bool {
	return p.EnvVars != nil
}

func (p *Command) IsSetTimeout() bool {
	return p.Timeout != nil
}

func (p *Command) Read(iprot thrift.TProtocol) error {
	if _, err := iprot.ReadStructBegin(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
	}

	var issetArgv bool = false
	var issetSnapshotId bool = false

	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
		}
		if fieldTypeId == thrift.STOP {
			break
		}
		switch fieldId {
		case 1:
			if err := p.readField1(iprot); err != nil {
				return err
			}
			issetArgv = true
		case 2:
			if err := p.readField2(iprot); err != nil {
				return err
			}
		case 3:
			if err := p.readField3(iprot); err != nil {
				return err
			}
		case 4:
			if err := p.readField4(iprot); err != nil {
				return err
			}
			issetSnapshotId = true
		default:
			if err := iprot.Skip(fieldTypeId); err != nil {
				return err
			}
		}
		if err := iprot.ReadFieldEnd(); err != nil {
			return err
		}
	}
	if err := iprot.ReadStructEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
	}
	if !issetArgv {
		return thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, fmt.Errorf("Required field Argv is not set"))
	}
	if !issetSnapshotId {
		return thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, fmt.Errorf("Required field SnapshotId is not set"))
	}
	return nil
}

func (p *Command) readField1(iprot thrift.TProtocol) error {
	_, size, err := iprot.ReadListBegin()
	if err != nil {
		return thrift.PrependError("error reading list begin: ", err)
	}
	tSlice := make([]string, 0, size)
	p.Argv = tSlice
	for i := 0; i < size; i++ {
		var _elem0 string
		if v, err := iprot.ReadString(); err != nil {
			return thrift.PrependError("error reading field 0: ", err)
		} else {
			_elem0 = v
		}
		p.Argv = append(p.Argv, _elem0)
	}
	if err := iprot.ReadListEnd(); err != nil {
		return thrift.PrependError("error reading list end: ", err)
	}
	return nil
}

func (p *Command) readField2(iprot thrift.TProtocol) error {
	_, _, size, err := iprot.ReadMapBegin()
	if err != nil {
		return thrift.PrependError("error reading map begin: ", err)
	}
	tMap := make(map[string]string, size)
	p.EnvVars = tMap
	for i := 0; i < size; i++ {
		var _key1 string
		if v, err := iprot.ReadString(); err != nil {
			return thrift.PrependError("error reading field 0: ", err)
		} else {
			_key1 = v
		}
		var _val2 string
		if v, err := iprot.ReadString(); err != nil {
			return thrift.PrependError("error reading field 0: ", err)
		} else {
			_val2 = v
		}
		p.EnvVars[_key1] = _val2
	}
	if err := iprot.ReadMapEnd(); err != nil {
		return thrift.PrependError("error reading map end: ", err)
	}
	return nil
}

func (p *Command) readField3(iprot thrift.TProtocol) error {
	if v, err := iprot.ReadI64(); err != nil {
		return thrift.PrependError("error reading field 3: ", err)
	} else {
		p.Timeout = &v
	}
	return nil
}

func (p *Command) readField4(iprot thrift.TProtocol) error {
	if v, err := iprot.ReadString(); err != nil {
		return thrift.PrependError("error reading field 4: ", err)
	} else {
		p.SnapshotId = v
	}
	return nil
}

func (p *Command) Write(oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin("Command"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err)
	}
	if err := p.writeField1(oprot); err != nil {
		return err
	}
	if err := p.writeField2(oprot); err != nil {
		return err
	}
	if err := p.writeField3(oprot); err != nil {
		return err
	}
	if err := p.writeField4(oprot); err != nil {
		return err
	}
	if err := oprot.WriteFieldStop(); err != nil {
		return thrift.PrependError("write field stop error: ", err)
	}
	if err := oprot.WriteStructEnd(); err != nil {
		return thrift.PrependError("write struct stop error: ", err)
	}
	return nil
}

func (p *Command) writeField1(oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteFieldBegin("argv", thrift.LIST, 1); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:argv: ", p), err)
	}
	if err := oprot.WriteListBegin(thrift.STRING, len(p.Argv)); err != nil {
		return thrift.PrependError("error writing list begin: ", err)
	}
	for _, v := range p.Argv {
		if err := oprot.WriteString(string(v)); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T. (0) field write error: ", p), err)
		}
	}
	if err := oprot.WriteListEnd(); err != nil {
		return thrift.PrependError("error writing list end: ", err)
	}
	if err := oprot.WriteFieldEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field end error 1:argv: ", p), err)
	}
	return err
}

func (p *Command) writeField2(oprot thrift.TProtocol) (err error) {
	if p.IsSetEnvVars() {
		if err := oprot.WriteFieldBegin("envVars", thrift.MAP, 2); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 2:envVars: ", p), err)
		}
		if err := oprot.WriteMapBegin(thrift.STRING, thrift.STRING, len(p.EnvVars)); err != nil {
			return thrift.PrependError("error writing map begin: ", err)
		}
		for k, v := range p.EnvVars {
			if err := oprot.WriteString(string(k)); err != nil {
				return thrift.PrependError(fmt.Sprintf("%T. (0) field write error: ", p), err)
			}
			if err := oprot.WriteString(string(v)); err != nil {
				return thrift.PrependError(fmt.Sprintf("%T. (0) field write error: ", p), err)
			}
		}
		if err := oprot.WriteMapEnd(); err != nil {
			return thrift.PrependError("error writing map end: ", err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 2:envVars: ", p), err)
		}
	}
	return err
}

func (p *Command) writeField3(oprot thrift.TProtocol) (err error) {
	if p.IsSetTimeout() {
		if err := oprot.WriteFieldBegin("timeout", thrift.I64, 3); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 3:timeout: ", p), err)
		}
		if err := oprot.WriteI64(int64(*p.Timeout)); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T.timeout (3) field write error: ", p), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 3:timeout: ", p), err)
		}
	}
	return err
}

func (p *Command) writeField4(oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteFieldBegin("snapshotId", thrift.STRING, 4); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field begin error 4:snapshotId: ", p), err)
	}
	if err := oprot.WriteString(string(p.SnapshotId)); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T.snapshotId (4) field write error: ", p), err)
	}
	if err := oprot.WriteFieldEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field end error 4:snapshotId: ", p), err)
	}
	return err
}

func (p *Command) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Command(%+v)", *p)
}

// Attributes:
//  - Command
//  - TaskId
//  - JobId
type TaskDefinition struct {
	Command *Command `thrift:"command,1,required" json:"command"`
	TaskId  *string  `thrift:"taskId,2" json:"taskId,omitempty"`
	JobId   *string  `thrift:"jobId,3" json:"jobId,omitempty"`
}

func NewTaskDefinition() *TaskDefinition {
	return &TaskDefinition{}
}

var TaskDefinition_Command_DEFAULT *Command

func (p *TaskDefinition) GetCommand() *Command {
	if !p.IsSetCommand() {
		return TaskDefinition_Command_DEFAULT
	}
	return p.Command
}

var TaskDefinition_TaskId_DEFAULT string

func (p *TaskDefinition) GetTaskId() string {
	if !p.IsSetTaskId() {
		return TaskDefinition_TaskId_DEFAULT
	}
	return *p.TaskId
}

var TaskDefinition_JobId_DEFAULT string

func (p *TaskDefinition) GetJobId() string {
	if !p.IsSetJobId() {
		return TaskDefinition_JobId_DEFAULT
	}
	return *p.JobId
}
func (p *TaskDefinition) IsSetCommand() bool {
	return p.Command != nil
}

func (p *TaskDefinition) IsSetTaskId() bool {
	return p.TaskId != nil
}

func (p *TaskDefinition) IsSetJobId() bool {
	return p.JobId != nil
}

func (p *TaskDefinition) Read(iprot thrift.TProtocol) error {
	if _, err := iprot.ReadStructBegin(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
	}

	var issetCommand bool = false

	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
		}
		if fieldTypeId == thrift.STOP {
			break
		}
		switch fieldId {
		case 1:
			if err := p.readField1(iprot); err != nil {
				return err
			}
			issetCommand = true
		case 2:
			if err := p.readField2(iprot); err != nil {
				return err
			}
		case 3:
			if err := p.readField3(iprot); err != nil {
				return err
			}
		default:
			if err := iprot.Skip(fieldTypeId); err != nil {
				return err
			}
		}
		if err := iprot.ReadFieldEnd(); err != nil {
			return err
		}
	}
	if err := iprot.ReadStructEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
	}
	if !issetCommand {
		return thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, fmt.Errorf("Required field Command is not set"))
	}
	return nil
}

func (p *TaskDefinition) readField1(iprot thrift.TProtocol) error {
	p.Command = &Command{}
	if err := p.Command.Read(iprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Command), err)
	}
	return nil
}

func (p *TaskDefinition) readField2(iprot thrift.TProtocol) error {
	if v, err := iprot.ReadString(); err != nil {
		return thrift.PrependError("error reading field 2: ", err)
	} else {
		p.TaskId = &v
	}
	return nil
}

func (p *TaskDefinition) readField3(iprot thrift.TProtocol) error {
	if v, err := iprot.ReadString(); err != nil {
		return thrift.PrependError("error reading field 3: ", err)
	} else {
		p.JobId = &v
	}
	return nil
}

func (p *TaskDefinition) Write(oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin("TaskDefinition"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err)
	}
	if err := p.writeField1(oprot); err != nil {
		return err
	}
	if err := p.writeField2(oprot); err != nil {
		return err
	}
	if err := p.writeField3(oprot); err != nil {
		return err
	}
	if err := oprot.WriteFieldStop(); err != nil {
		return thrift.PrependError("write field stop error: ", err)
	}
	if err := oprot.WriteStructEnd(); err != nil {
		return thrift.PrependError("write struct stop error: ", err)
	}
	return nil
}

func (p *TaskDefinition) writeField1(oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteFieldBegin("command", thrift.STRUCT, 1); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:command: ", p), err)
	}
	if err := p.Command.Write(oprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Command), err)
	}
	if err := oprot.WriteFieldEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field end error 1:command: ", p), err)
	}
	return err
}

func (p *TaskDefinition) writeField2(oprot thrift.TProtocol) (err error) {
	if p.IsSetTaskId() {
		if err := oprot.WriteFieldBegin("taskId", thrift.STRING, 2); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 2:taskId: ", p), err)
		}
		if err := oprot.WriteString(string(*p.TaskId)); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T.taskId (2) field write error: ", p), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 2:taskId: ", p), err)
		}
	}
	return err
}

func (p *TaskDefinition) writeField3(oprot thrift.TProtocol) (err error) {
	if p.IsSetJobId() {
		if err := oprot.WriteFieldBegin("jobId", thrift.STRING, 3); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 3:jobId: ", p), err)
		}
		if err := oprot.WriteString(string(*p.JobId)); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T.jobId (3) field write error: ", p), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 3:jobId: ", p), err)
		}
	}
	return err
}

func (p *TaskDefinition) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("TaskDefinition(%+v)", *p)
}

// Attributes:
//  - JobType
//  - Tasks
//  - Priority
//  - Tag
//  - Basis
//  - Requestor
type JobDefinition struct {
	JobType   *string           `thrift:"jobType,1" json:"jobType,omitempty"`
	Tasks     []*TaskDefinition `thrift:"tasks,2" json:"tasks,omitempty"`
	Priority  *int32            `thrift:"priority,3" json:"priority,omitempty"`
	Tag       *string           `thrift:"tag,4" json:"tag,omitempty"`
	Basis     *string           `thrift:"basis,5" json:"basis,omitempty"`
	Requestor *string           `thrift:"requestor,6" json:"requestor,omitempty"`
}

func NewJobDefinition() *JobDefinition {
	return &JobDefinition{}
}

var JobDefinition_JobType_DEFAULT string

func (p *JobDefinition) GetJobType() string {
	if !p.IsSetJobType() {
		return JobDefinition_JobType_DEFAULT
	}
	return *p.JobType
}

var JobDefinition_Tasks_DEFAULT []*TaskDefinition

func (p *JobDefinition) GetTasks() []*TaskDefinition {
	return p.Tasks
}

var JobDefinition_Priority_DEFAULT int32

func (p *JobDefinition) GetPriority() int32 {
	if !p.IsSetPriority() {
		return JobDefinition_Priority_DEFAULT
	}
	return *p.Priority
}

var JobDefinition_Tag_DEFAULT string

func (p *JobDefinition) GetTag() string {
	if !p.IsSetTag() {
		return JobDefinition_Tag_DEFAULT
	}
	return *p.Tag
}

var JobDefinition_Basis_DEFAULT string

func (p *JobDefinition) GetBasis() string {
	if !p.IsSetBasis() {
		return JobDefinition_Basis_DEFAULT
	}
	return *p.Basis
}

var JobDefinition_Requestor_DEFAULT string

func (p *JobDefinition) GetRequestor() string {
	if !p.IsSetRequestor() {
		return JobDefinition_Requestor_DEFAULT
	}
	return *p.Requestor
}
func (p *JobDefinition) IsSetJobType() bool {
	return p.JobType != nil
}

func (p *JobDefinition) IsSetTasks() bool {
	return p.Tasks != nil
}

func (p *JobDefinition) IsSetPriority() bool {
	return p.Priority != nil
}

func (p *JobDefinition) IsSetTag() bool {
	return p.Tag != nil
}

func (p *JobDefinition) IsSetBasis() bool {
	return p.Basis != nil
}

func (p *JobDefinition) IsSetRequestor() bool {
	return p.Requestor != nil
}

func (p *JobDefinition) Read(iprot thrift.TProtocol) error {
	if _, err := iprot.ReadStructBegin(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
	}

	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
		}
		if fieldTypeId == thrift.STOP {
			break
		}
		switch fieldId {
		case 1:
			if err := p.readField1(iprot); err != nil {
				return err
			}
		case 2:
			if err := p.readField2(iprot); err != nil {
				return err
			}
		case 3:
			if err := p.readField3(iprot); err != nil {
				return err
			}
		case 4:
			if err := p.readField4(iprot); err != nil {
				return err
			}
		case 5:
			if err := p.readField5(iprot); err != nil {
				return err
			}
		case 6:
			if err := p.readField6(iprot); err != nil {
				return err
			}
		default:
			if err := iprot.Skip(fieldTypeId); err != nil {
				return err
			}
		}
		if err := iprot.ReadFieldEnd(); err != nil {
			return err
		}
	}
	if err := iprot.ReadStructEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
	}
	return nil
}

func (p *JobDefinition) readField1(iprot thrift.TProtocol) error {
	if v, err := iprot.ReadString(); err != nil {
		return thrift.PrependError("error reading field 1: ", err)
	} else {
		p.JobType = &v
	}
	return nil
}

func (p *JobDefinition) readField2(iprot thrift.TProtocol) error {
	_, size, err := iprot.ReadListBegin()
	if err != nil {
		return thrift.PrependError("error reading list begin: ", err)
	}
	tSlice := make([]*TaskDefinition, 0, size)
	p.Tasks = tSlice
	for i := 0; i < size; i++ {
		_elem3 := &TaskDefinition{}
		if err := _elem3.Read(iprot); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", _elem3), err)
		}
		p.Tasks = append(p.Tasks, _elem3)
	}
	if err := iprot.ReadListEnd(); err != nil {
		return thrift.PrependError("error reading list end: ", err)
	}
	return nil
}

func (p *JobDefinition) readField3(iprot thrift.TProtocol) error {
	if v, err := iprot.ReadI32(); err != nil {
		return thrift.PrependError("error reading field 3: ", err)
	} else {
		p.Priority = &v
	}
	return nil
}

func (p *JobDefinition) readField4(iprot thrift.TProtocol) error {
	if v, err := iprot.ReadString(); err != nil {
		return thrift.PrependError("error reading field 4: ", err)
	} else {
		p.Tag = &v
	}
	return nil
}

func (p *JobDefinition) readField5(iprot thrift.TProtocol) error {
	if v, err := iprot.ReadString(); err != nil {
		return thrift.PrependError("error reading field 5: ", err)
	} else {
		p.Basis = &v
	}
	return nil
}

func (p *JobDefinition) readField6(iprot thrift.TProtocol) error {
	if v, err := iprot.ReadString(); err != nil {
		return thrift.PrependError("error reading field 6: ", err)
	} else {
		p.Requestor = &v
	}
	return nil
}

func (p *JobDefinition) Write(oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin("JobDefinition"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err)
	}
	if err := p.writeField1(oprot); err != nil {
		return err
	}
	if err := p.writeField2(oprot); err != nil {
		return err
	}
	if err := p.writeField3(oprot); err != nil {
		return err
	}
	if err := p.writeField4(oprot); err != nil {
		return err
	}
	if err := p.writeField5(oprot); err != nil {
		return err
	}
	if err := p.writeField6(oprot); err != nil {
		return err
	}
	if err := oprot.WriteFieldStop(); err != nil {
		return thrift.PrependError("write field stop error: ", err)
	}
	if err := oprot.WriteStructEnd(); err != nil {
		return thrift.PrependError("write struct stop error: ", err)
	}
	return nil
}

func (p *JobDefinition) writeField1(oprot thrift.TProtocol) (err error) {
	if p.IsSetJobType() {
		if err := oprot.WriteFieldBegin("jobType", thrift.STRING, 1); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:jobType: ", p), err)
		}
		if err := oprot.WriteString(string(*p.JobType)); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T.jobType (1) field write error: ", p), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 1:jobType: ", p), err)
		}
	}
	return err
}

func (p *JobDefinition) writeField2(oprot thrift.TProtocol) (err error) {
	if p.IsSetTasks() {
		if err := oprot.WriteFieldBegin("tasks", thrift.LIST, 2); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 2:tasks: ", p), err)
		}
		if err := oprot.WriteListBegin(thrift.STRUCT, len(p.Tasks)); err != nil {
			return thrift.PrependError("error writing list begin: ", err)
		}
		for _, v := range p.Tasks {
			if err := v.Write(oprot); err != nil {
				return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", v), err)
			}
		}
		if err := oprot.WriteListEnd(); err != nil {
			return thrift.PrependError("error writing list end: ", err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 2:tasks: ", p), err)
		}
	}
	return err
}

func (p *JobDefinition) writeField3(oprot thrift.TProtocol) (err error) {
	if p.IsSetPriority() {
		if err := oprot.WriteFieldBegin("priority", thrift.I32, 3); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 3:priority: ", p), err)
		}
		if err := oprot.WriteI32(int32(*p.Priority)); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T.priority (3) field write error: ", p), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 3:priority: ", p), err)
		}
	}
	return err
}

func (p *JobDefinition) writeField4(oprot thrift.TProtocol) (err error) {
	if p.IsSetTag() {
		if err := oprot.WriteFieldBegin("tag", thrift.STRING, 4); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 4:tag: ", p), err)
		}
		if err := oprot.WriteString(string(*p.Tag)); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T.tag (4) field write error: ", p), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 4:tag: ", p), err)
		}
	}
	return err
}

func (p *JobDefinition) writeField5(oprot thrift.TProtocol) (err error) {
	if p.IsSetBasis() {
		if err := oprot.WriteFieldBegin("basis", thrift.STRING, 5); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 5:basis: ", p), err)
		}
		if err := oprot.WriteString(string(*p.Basis)); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T.basis (5) field write error: ", p), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 5:basis: ", p), err)
		}
	}
	return err
}

func (p *JobDefinition) writeField6(oprot thrift.TProtocol) (err error) {
	if p.IsSetRequestor() {
		if err := oprot.WriteFieldBegin("requestor", thrift.STRING, 6); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 6:requestor: ", p), err)
		}
		if err := oprot.WriteString(string(*p.Requestor)); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T.requestor (6) field write error: ", p), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 6:requestor: ", p), err)
		}
	}
	return err
}

func (p *JobDefinition) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("JobDefinition(%+v)", *p)
}

// Attributes:
//  - ID
//  - JobDefinition
type Job struct {
	ID            string         `thrift:"id,1,required" json:"id"`
	JobDefinition *JobDefinition `thrift:"jobDefinition,2,required" json:"jobDefinition"`
}

func NewJob() *Job {
	return &Job{}
}

func (p *Job) GetID() string {
	return p.ID
}

var Job_JobDefinition_DEFAULT *JobDefinition

func (p *Job) GetJobDefinition() *JobDefinition {
	if !p.IsSetJobDefinition() {
		return Job_JobDefinition_DEFAULT
	}
	return p.JobDefinition
}
func (p *Job) IsSetJobDefinition() bool {
	return p.JobDefinition != nil
}

func (p *Job) Read(iprot thrift.TProtocol) error {
	if _, err := iprot.ReadStructBegin(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read error: ", p), err)
	}

	var issetID bool = false
	var issetJobDefinition bool = false

	for {
		_, fieldTypeId, fieldId, err := iprot.ReadFieldBegin()
		if err != nil {
			return thrift.PrependError(fmt.Sprintf("%T field %d read error: ", p, fieldId), err)
		}
		if fieldTypeId == thrift.STOP {
			break
		}
		switch fieldId {
		case 1:
			if err := p.readField1(iprot); err != nil {
				return err
			}
			issetID = true
		case 2:
			if err := p.readField2(iprot); err != nil {
				return err
			}
			issetJobDefinition = true
		default:
			if err := iprot.Skip(fieldTypeId); err != nil {
				return err
			}
		}
		if err := iprot.ReadFieldEnd(); err != nil {
			return err
		}
	}
	if err := iprot.ReadStructEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T read struct end error: ", p), err)
	}
	if !issetID {
		return thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, fmt.Errorf("Required field ID is not set"))
	}
	if !issetJobDefinition {
		return thrift.NewTProtocolExceptionWithType(thrift.INVALID_DATA, fmt.Errorf("Required field JobDefinition is not set"))
	}
	return nil
}

func (p *Job) readField1(iprot thrift.TProtocol) error {
	if v, err := iprot.ReadString(); err != nil {
		return thrift.PrependError("error reading field 1: ", err)
	} else {
		p.ID = v
	}
	return nil
}

func (p *Job) readField2(iprot thrift.TProtocol) error {
	p.JobDefinition = &JobDefinition{}
	if err := p.JobDefinition.Read(iprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.JobDefinition), err)
	}
	return nil
}

func (p *Job) Write(oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin("Job"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err)
	}
	if err := p.writeField1(oprot); err != nil {
		return err
	}
	if err := p.writeField2(oprot); err != nil {
		return err
	}
	if err := oprot.WriteFieldStop(); err != nil {
		return thrift.PrependError("write field stop error: ", err)
	}
	if err := oprot.WriteStructEnd(); err != nil {
		return thrift.PrependError("write struct stop error: ", err)
	}
	return nil
}

func (p *Job) writeField1(oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteFieldBegin("id", thrift.STRING, 1); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:id: ", p), err)
	}
	if err := oprot.WriteString(string(p.ID)); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T.id (1) field write error: ", p), err)
	}
	if err := oprot.WriteFieldEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field end error 1:id: ", p), err)
	}
	return err
}

func (p *Job) writeField2(oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteFieldBegin("jobDefinition", thrift.STRUCT, 2); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field begin error 2:jobDefinition: ", p), err)
	}
	if err := p.JobDefinition.Write(oprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.JobDefinition), err)
	}
	if err := oprot.WriteFieldEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field end error 2:jobDefinition: ", p), err)
	}
	return err
}

func (p *Job) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Job(%+v)", *p)
}
