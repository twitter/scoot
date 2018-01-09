// Autogenerated by Thrift Compiler (0.9.3)
// DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING

package scoot

import (
	"bytes"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/twitter/scoot/bazel/execution/request/gen-go/request"
)

// (needed to ensure safety because of naive import list construction.)
var _ = thrift.ZERO
var _ = fmt.Printf
var _ = bytes.Equal

var _ = request.GoUnusedProtection__

type CloudScoot interface {
	// Parameters:
	//  - Job
	RunJob(job *JobDefinition) (r *JobId, err error)
	// Parameters:
	//  - JobId
	GetStatus(jobId string) (r *JobStatus, err error)
	// Parameters:
	//  - JobId
	KillJob(jobId string) (r *JobStatus, err error)
}

type CloudScootClient struct {
	Transport       thrift.TTransport
	ProtocolFactory thrift.TProtocolFactory
	InputProtocol   thrift.TProtocol
	OutputProtocol  thrift.TProtocol
	SeqId           int32
}

func NewCloudScootClientFactory(t thrift.TTransport, f thrift.TProtocolFactory) *CloudScootClient {
	return &CloudScootClient{Transport: t,
		ProtocolFactory: f,
		InputProtocol:   f.GetProtocol(t),
		OutputProtocol:  f.GetProtocol(t),
		SeqId:           0,
	}
}

func NewCloudScootClientProtocol(t thrift.TTransport, iprot thrift.TProtocol, oprot thrift.TProtocol) *CloudScootClient {
	return &CloudScootClient{Transport: t,
		ProtocolFactory: nil,
		InputProtocol:   iprot,
		OutputProtocol:  oprot,
		SeqId:           0,
	}
}

// Parameters:
//  - Job
func (p *CloudScootClient) RunJob(job *JobDefinition) (r *JobId, err error) {
	if err = p.sendRunJob(job); err != nil {
		return
	}
	return p.recvRunJob()
}

func (p *CloudScootClient) sendRunJob(job *JobDefinition) (err error) {
	oprot := p.OutputProtocol
	if oprot == nil {
		oprot = p.ProtocolFactory.GetProtocol(p.Transport)
		p.OutputProtocol = oprot
	}
	p.SeqId++
	if err = oprot.WriteMessageBegin("RunJob", thrift.CALL, p.SeqId); err != nil {
		return
	}
	args := CloudScootRunJobArgs{
		Job: job,
	}
	if err = args.Write(oprot); err != nil {
		return
	}
	if err = oprot.WriteMessageEnd(); err != nil {
		return
	}
	return oprot.Flush()
}

func (p *CloudScootClient) recvRunJob() (value *JobId, err error) {
	iprot := p.InputProtocol
	if iprot == nil {
		iprot = p.ProtocolFactory.GetProtocol(p.Transport)
		p.InputProtocol = iprot
	}
	method, mTypeId, seqId, err := iprot.ReadMessageBegin()
	if err != nil {
		return
	}
	if method != "RunJob" {
		err = thrift.NewTApplicationException(thrift.WRONG_METHOD_NAME, "RunJob failed: wrong method name")
		return
	}
	if p.SeqId != seqId {
		err = thrift.NewTApplicationException(thrift.BAD_SEQUENCE_ID, "RunJob failed: out of sequence response")
		return
	}
	if mTypeId == thrift.EXCEPTION {
		error8 := thrift.NewTApplicationException(thrift.UNKNOWN_APPLICATION_EXCEPTION, "Unknown Exception")
		var error9 error
		error9, err = error8.Read(iprot)
		if err != nil {
			return
		}
		if err = iprot.ReadMessageEnd(); err != nil {
			return
		}
		err = error9
		return
	}
	if mTypeId != thrift.REPLY {
		err = thrift.NewTApplicationException(thrift.INVALID_MESSAGE_TYPE_EXCEPTION, "RunJob failed: invalid message type")
		return
	}
	result := CloudScootRunJobResult{}
	if err = result.Read(iprot); err != nil {
		return
	}
	if err = iprot.ReadMessageEnd(); err != nil {
		return
	}
	if result.Ir != nil {
		err = result.Ir
		return
	} else if result.Cnsn != nil {
		err = result.Cnsn
		return
	}
	value = result.GetSuccess()
	return
}

// Parameters:
//  - JobId
func (p *CloudScootClient) GetStatus(jobId string) (r *JobStatus, err error) {
	if err = p.sendGetStatus(jobId); err != nil {
		return
	}
	return p.recvGetStatus()
}

func (p *CloudScootClient) sendGetStatus(jobId string) (err error) {
	oprot := p.OutputProtocol
	if oprot == nil {
		oprot = p.ProtocolFactory.GetProtocol(p.Transport)
		p.OutputProtocol = oprot
	}
	p.SeqId++
	if err = oprot.WriteMessageBegin("GetStatus", thrift.CALL, p.SeqId); err != nil {
		return
	}
	args := CloudScootGetStatusArgs{
		JobId: jobId,
	}
	if err = args.Write(oprot); err != nil {
		return
	}
	if err = oprot.WriteMessageEnd(); err != nil {
		return
	}
	return oprot.Flush()
}

func (p *CloudScootClient) recvGetStatus() (value *JobStatus, err error) {
	iprot := p.InputProtocol
	if iprot == nil {
		iprot = p.ProtocolFactory.GetProtocol(p.Transport)
		p.InputProtocol = iprot
	}
	method, mTypeId, seqId, err := iprot.ReadMessageBegin()
	if err != nil {
		return
	}
	if method != "GetStatus" {
		err = thrift.NewTApplicationException(thrift.WRONG_METHOD_NAME, "GetStatus failed: wrong method name")
		return
	}
	if p.SeqId != seqId {
		err = thrift.NewTApplicationException(thrift.BAD_SEQUENCE_ID, "GetStatus failed: out of sequence response")
		return
	}
	if mTypeId == thrift.EXCEPTION {
		error10 := thrift.NewTApplicationException(thrift.UNKNOWN_APPLICATION_EXCEPTION, "Unknown Exception")
		var error11 error
		error11, err = error10.Read(iprot)
		if err != nil {
			return
		}
		if err = iprot.ReadMessageEnd(); err != nil {
			return
		}
		err = error11
		return
	}
	if mTypeId != thrift.REPLY {
		err = thrift.NewTApplicationException(thrift.INVALID_MESSAGE_TYPE_EXCEPTION, "GetStatus failed: invalid message type")
		return
	}
	result := CloudScootGetStatusResult{}
	if err = result.Read(iprot); err != nil {
		return
	}
	if err = iprot.ReadMessageEnd(); err != nil {
		return
	}
	if result.Ir != nil {
		err = result.Ir
		return
	} else if result.Err != nil {
		err = result.Err
		return
	}
	value = result.GetSuccess()
	return
}

// Parameters:
//  - JobId
func (p *CloudScootClient) KillJob(jobId string) (r *JobStatus, err error) {
	if err = p.sendKillJob(jobId); err != nil {
		return
	}
	return p.recvKillJob()
}

func (p *CloudScootClient) sendKillJob(jobId string) (err error) {
	oprot := p.OutputProtocol
	if oprot == nil {
		oprot = p.ProtocolFactory.GetProtocol(p.Transport)
		p.OutputProtocol = oprot
	}
	p.SeqId++
	if err = oprot.WriteMessageBegin("KillJob", thrift.CALL, p.SeqId); err != nil {
		return
	}
	args := CloudScootKillJobArgs{
		JobId: jobId,
	}
	if err = args.Write(oprot); err != nil {
		return
	}
	if err = oprot.WriteMessageEnd(); err != nil {
		return
	}
	return oprot.Flush()
}

func (p *CloudScootClient) recvKillJob() (value *JobStatus, err error) {
	iprot := p.InputProtocol
	if iprot == nil {
		iprot = p.ProtocolFactory.GetProtocol(p.Transport)
		p.InputProtocol = iprot
	}
	method, mTypeId, seqId, err := iprot.ReadMessageBegin()
	if err != nil {
		return
	}
	if method != "KillJob" {
		err = thrift.NewTApplicationException(thrift.WRONG_METHOD_NAME, "KillJob failed: wrong method name")
		return
	}
	if p.SeqId != seqId {
		err = thrift.NewTApplicationException(thrift.BAD_SEQUENCE_ID, "KillJob failed: out of sequence response")
		return
	}
	if mTypeId == thrift.EXCEPTION {
		error12 := thrift.NewTApplicationException(thrift.UNKNOWN_APPLICATION_EXCEPTION, "Unknown Exception")
		var error13 error
		error13, err = error12.Read(iprot)
		if err != nil {
			return
		}
		if err = iprot.ReadMessageEnd(); err != nil {
			return
		}
		err = error13
		return
	}
	if mTypeId != thrift.REPLY {
		err = thrift.NewTApplicationException(thrift.INVALID_MESSAGE_TYPE_EXCEPTION, "KillJob failed: invalid message type")
		return
	}
	result := CloudScootKillJobResult{}
	if err = result.Read(iprot); err != nil {
		return
	}
	if err = iprot.ReadMessageEnd(); err != nil {
		return
	}
	if result.Ir != nil {
		err = result.Ir
		return
	} else if result.Err != nil {
		err = result.Err
		return
	}
	value = result.GetSuccess()
	return
}

type CloudScootProcessor struct {
	processorMap map[string]thrift.TProcessorFunction
	handler      CloudScoot
}

func (p *CloudScootProcessor) AddToProcessorMap(key string, processor thrift.TProcessorFunction) {
	p.processorMap[key] = processor
}

func (p *CloudScootProcessor) GetProcessorFunction(key string) (processor thrift.TProcessorFunction, ok bool) {
	processor, ok = p.processorMap[key]
	return processor, ok
}

func (p *CloudScootProcessor) ProcessorMap() map[string]thrift.TProcessorFunction {
	return p.processorMap
}

func NewCloudScootProcessor(handler CloudScoot) *CloudScootProcessor {

	self14 := &CloudScootProcessor{handler: handler, processorMap: make(map[string]thrift.TProcessorFunction)}
	self14.processorMap["RunJob"] = &cloudScootProcessorRunJob{handler: handler}
	self14.processorMap["GetStatus"] = &cloudScootProcessorGetStatus{handler: handler}
	self14.processorMap["KillJob"] = &cloudScootProcessorKillJob{handler: handler}
	return self14
}

func (p *CloudScootProcessor) Process(iprot, oprot thrift.TProtocol) (success bool, err thrift.TException) {
	name, _, seqId, err := iprot.ReadMessageBegin()
	if err != nil {
		return false, err
	}
	if processor, ok := p.GetProcessorFunction(name); ok {
		return processor.Process(seqId, iprot, oprot)
	}
	iprot.Skip(thrift.STRUCT)
	iprot.ReadMessageEnd()
	x15 := thrift.NewTApplicationException(thrift.UNKNOWN_METHOD, "Unknown function "+name)
	oprot.WriteMessageBegin(name, thrift.EXCEPTION, seqId)
	x15.Write(oprot)
	oprot.WriteMessageEnd()
	oprot.Flush()
	return false, x15

}

type cloudScootProcessorRunJob struct {
	handler CloudScoot
}

func (p *cloudScootProcessorRunJob) Process(seqId int32, iprot, oprot thrift.TProtocol) (success bool, err thrift.TException) {
	args := CloudScootRunJobArgs{}
	if err = args.Read(iprot); err != nil {
		iprot.ReadMessageEnd()
		x := thrift.NewTApplicationException(thrift.PROTOCOL_ERROR, err.Error())
		oprot.WriteMessageBegin("RunJob", thrift.EXCEPTION, seqId)
		x.Write(oprot)
		oprot.WriteMessageEnd()
		oprot.Flush()
		return false, err
	}

	iprot.ReadMessageEnd()
	result := CloudScootRunJobResult{}
	var retval *JobId
	var err2 error
	if retval, err2 = p.handler.RunJob(args.Job); err2 != nil {
		switch v := err2.(type) {
		case *InvalidRequest:
			result.Ir = v
		case *CanNotScheduleNow:
			result.Cnsn = v
		default:
			x := thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "Internal error processing RunJob: "+err2.Error())
			oprot.WriteMessageBegin("RunJob", thrift.EXCEPTION, seqId)
			x.Write(oprot)
			oprot.WriteMessageEnd()
			oprot.Flush()
			return true, err2
		}
	} else {
		result.Success = retval
	}
	if err2 = oprot.WriteMessageBegin("RunJob", thrift.REPLY, seqId); err2 != nil {
		err = err2
	}
	if err2 = result.Write(oprot); err == nil && err2 != nil {
		err = err2
	}
	if err2 = oprot.WriteMessageEnd(); err == nil && err2 != nil {
		err = err2
	}
	if err2 = oprot.Flush(); err == nil && err2 != nil {
		err = err2
	}
	if err != nil {
		return
	}
	return true, err
}

type cloudScootProcessorGetStatus struct {
	handler CloudScoot
}

func (p *cloudScootProcessorGetStatus) Process(seqId int32, iprot, oprot thrift.TProtocol) (success bool, err thrift.TException) {
	args := CloudScootGetStatusArgs{}
	if err = args.Read(iprot); err != nil {
		iprot.ReadMessageEnd()
		x := thrift.NewTApplicationException(thrift.PROTOCOL_ERROR, err.Error())
		oprot.WriteMessageBegin("GetStatus", thrift.EXCEPTION, seqId)
		x.Write(oprot)
		oprot.WriteMessageEnd()
		oprot.Flush()
		return false, err
	}

	iprot.ReadMessageEnd()
	result := CloudScootGetStatusResult{}
	var retval *JobStatus
	var err2 error
	if retval, err2 = p.handler.GetStatus(args.JobId); err2 != nil {
		switch v := err2.(type) {
		case *InvalidRequest:
			result.Ir = v
		case *ScootServerError:
			result.Err = v
		default:
			x := thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "Internal error processing GetStatus: "+err2.Error())
			oprot.WriteMessageBegin("GetStatus", thrift.EXCEPTION, seqId)
			x.Write(oprot)
			oprot.WriteMessageEnd()
			oprot.Flush()
			return true, err2
		}
	} else {
		result.Success = retval
	}
	if err2 = oprot.WriteMessageBegin("GetStatus", thrift.REPLY, seqId); err2 != nil {
		err = err2
	}
	if err2 = result.Write(oprot); err == nil && err2 != nil {
		err = err2
	}
	if err2 = oprot.WriteMessageEnd(); err == nil && err2 != nil {
		err = err2
	}
	if err2 = oprot.Flush(); err == nil && err2 != nil {
		err = err2
	}
	if err != nil {
		return
	}
	return true, err
}

type cloudScootProcessorKillJob struct {
	handler CloudScoot
}

func (p *cloudScootProcessorKillJob) Process(seqId int32, iprot, oprot thrift.TProtocol) (success bool, err thrift.TException) {
	args := CloudScootKillJobArgs{}
	if err = args.Read(iprot); err != nil {
		iprot.ReadMessageEnd()
		x := thrift.NewTApplicationException(thrift.PROTOCOL_ERROR, err.Error())
		oprot.WriteMessageBegin("KillJob", thrift.EXCEPTION, seqId)
		x.Write(oprot)
		oprot.WriteMessageEnd()
		oprot.Flush()
		return false, err
	}

	iprot.ReadMessageEnd()
	result := CloudScootKillJobResult{}
	var retval *JobStatus
	var err2 error
	if retval, err2 = p.handler.KillJob(args.JobId); err2 != nil {
		switch v := err2.(type) {
		case *InvalidRequest:
			result.Ir = v
		case *ScootServerError:
			result.Err = v
		default:
			x := thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "Internal error processing KillJob: "+err2.Error())
			oprot.WriteMessageBegin("KillJob", thrift.EXCEPTION, seqId)
			x.Write(oprot)
			oprot.WriteMessageEnd()
			oprot.Flush()
			return true, err2
		}
	} else {
		result.Success = retval
	}
	if err2 = oprot.WriteMessageBegin("KillJob", thrift.REPLY, seqId); err2 != nil {
		err = err2
	}
	if err2 = result.Write(oprot); err == nil && err2 != nil {
		err = err2
	}
	if err2 = oprot.WriteMessageEnd(); err == nil && err2 != nil {
		err = err2
	}
	if err2 = oprot.Flush(); err == nil && err2 != nil {
		err = err2
	}
	if err != nil {
		return
	}
	return true, err
}

// HELPER FUNCTIONS AND STRUCTURES

// Attributes:
//  - Job
type CloudScootRunJobArgs struct {
	Job *JobDefinition `thrift:"job,1" json:"job"`
}

func NewCloudScootRunJobArgs() *CloudScootRunJobArgs {
	return &CloudScootRunJobArgs{}
}

var CloudScootRunJobArgs_Job_DEFAULT *JobDefinition

func (p *CloudScootRunJobArgs) GetJob() *JobDefinition {
	if !p.IsSetJob() {
		return CloudScootRunJobArgs_Job_DEFAULT
	}
	return p.Job
}
func (p *CloudScootRunJobArgs) IsSetJob() bool {
	return p.Job != nil
}

func (p *CloudScootRunJobArgs) Read(iprot thrift.TProtocol) error {
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

func (p *CloudScootRunJobArgs) readField1(iprot thrift.TProtocol) error {
	p.Job = &JobDefinition{}
	if err := p.Job.Read(iprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Job), err)
	}
	return nil
}

func (p *CloudScootRunJobArgs) Write(oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin("RunJob_args"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err)
	}
	if err := p.writeField1(oprot); err != nil {
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

func (p *CloudScootRunJobArgs) writeField1(oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteFieldBegin("job", thrift.STRUCT, 1); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:job: ", p), err)
	}
	if err := p.Job.Write(oprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Job), err)
	}
	if err := oprot.WriteFieldEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field end error 1:job: ", p), err)
	}
	return err
}

func (p *CloudScootRunJobArgs) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("CloudScootRunJobArgs(%+v)", *p)
}

// Attributes:
//  - Success
//  - Ir
//  - Cnsn
type CloudScootRunJobResult struct {
	Success *JobId             `thrift:"success,0" json:"success,omitempty"`
	Ir      *InvalidRequest    `thrift:"ir,1" json:"ir,omitempty"`
	Cnsn    *CanNotScheduleNow `thrift:"cnsn,2" json:"cnsn,omitempty"`
}

func NewCloudScootRunJobResult() *CloudScootRunJobResult {
	return &CloudScootRunJobResult{}
}

var CloudScootRunJobResult_Success_DEFAULT *JobId

func (p *CloudScootRunJobResult) GetSuccess() *JobId {
	if !p.IsSetSuccess() {
		return CloudScootRunJobResult_Success_DEFAULT
	}
	return p.Success
}

var CloudScootRunJobResult_Ir_DEFAULT *InvalidRequest

func (p *CloudScootRunJobResult) GetIr() *InvalidRequest {
	if !p.IsSetIr() {
		return CloudScootRunJobResult_Ir_DEFAULT
	}
	return p.Ir
}

var CloudScootRunJobResult_Cnsn_DEFAULT *CanNotScheduleNow

func (p *CloudScootRunJobResult) GetCnsn() *CanNotScheduleNow {
	if !p.IsSetCnsn() {
		return CloudScootRunJobResult_Cnsn_DEFAULT
	}
	return p.Cnsn
}
func (p *CloudScootRunJobResult) IsSetSuccess() bool {
	return p.Success != nil
}

func (p *CloudScootRunJobResult) IsSetIr() bool {
	return p.Ir != nil
}

func (p *CloudScootRunJobResult) IsSetCnsn() bool {
	return p.Cnsn != nil
}

func (p *CloudScootRunJobResult) Read(iprot thrift.TProtocol) error {
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
		case 0:
			if err := p.readField0(iprot); err != nil {
				return err
			}
		case 1:
			if err := p.readField1(iprot); err != nil {
				return err
			}
		case 2:
			if err := p.readField2(iprot); err != nil {
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

func (p *CloudScootRunJobResult) readField0(iprot thrift.TProtocol) error {
	p.Success = &JobId{}
	if err := p.Success.Read(iprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Success), err)
	}
	return nil
}

func (p *CloudScootRunJobResult) readField1(iprot thrift.TProtocol) error {
	p.Ir = &InvalidRequest{}
	if err := p.Ir.Read(iprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Ir), err)
	}
	return nil
}

func (p *CloudScootRunJobResult) readField2(iprot thrift.TProtocol) error {
	p.Cnsn = &CanNotScheduleNow{}
	if err := p.Cnsn.Read(iprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Cnsn), err)
	}
	return nil
}

func (p *CloudScootRunJobResult) Write(oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin("RunJob_result"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err)
	}
	if err := p.writeField0(oprot); err != nil {
		return err
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

func (p *CloudScootRunJobResult) writeField0(oprot thrift.TProtocol) (err error) {
	if p.IsSetSuccess() {
		if err := oprot.WriteFieldBegin("success", thrift.STRUCT, 0); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 0:success: ", p), err)
		}
		if err := p.Success.Write(oprot); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Success), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 0:success: ", p), err)
		}
	}
	return err
}

func (p *CloudScootRunJobResult) writeField1(oprot thrift.TProtocol) (err error) {
	if p.IsSetIr() {
		if err := oprot.WriteFieldBegin("ir", thrift.STRUCT, 1); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:ir: ", p), err)
		}
		if err := p.Ir.Write(oprot); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Ir), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 1:ir: ", p), err)
		}
	}
	return err
}

func (p *CloudScootRunJobResult) writeField2(oprot thrift.TProtocol) (err error) {
	if p.IsSetCnsn() {
		if err := oprot.WriteFieldBegin("cnsn", thrift.STRUCT, 2); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 2:cnsn: ", p), err)
		}
		if err := p.Cnsn.Write(oprot); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Cnsn), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 2:cnsn: ", p), err)
		}
	}
	return err
}

func (p *CloudScootRunJobResult) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("CloudScootRunJobResult(%+v)", *p)
}

// Attributes:
//  - JobId
type CloudScootGetStatusArgs struct {
	JobId string `thrift:"jobId,1" json:"jobId"`
}

func NewCloudScootGetStatusArgs() *CloudScootGetStatusArgs {
	return &CloudScootGetStatusArgs{}
}

func (p *CloudScootGetStatusArgs) GetJobId() string {
	return p.JobId
}
func (p *CloudScootGetStatusArgs) Read(iprot thrift.TProtocol) error {
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

func (p *CloudScootGetStatusArgs) readField1(iprot thrift.TProtocol) error {
	if v, err := iprot.ReadString(); err != nil {
		return thrift.PrependError("error reading field 1: ", err)
	} else {
		p.JobId = v
	}
	return nil
}

func (p *CloudScootGetStatusArgs) Write(oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin("GetStatus_args"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err)
	}
	if err := p.writeField1(oprot); err != nil {
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

func (p *CloudScootGetStatusArgs) writeField1(oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteFieldBegin("jobId", thrift.STRING, 1); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:jobId: ", p), err)
	}
	if err := oprot.WriteString(string(p.JobId)); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T.jobId (1) field write error: ", p), err)
	}
	if err := oprot.WriteFieldEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field end error 1:jobId: ", p), err)
	}
	return err
}

func (p *CloudScootGetStatusArgs) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("CloudScootGetStatusArgs(%+v)", *p)
}

// Attributes:
//  - Success
//  - Ir
//  - Err
type CloudScootGetStatusResult struct {
	Success *JobStatus        `thrift:"success,0" json:"success,omitempty"`
	Ir      *InvalidRequest   `thrift:"ir,1" json:"ir,omitempty"`
	Err     *ScootServerError `thrift:"err,2" json:"err,omitempty"`
}

func NewCloudScootGetStatusResult() *CloudScootGetStatusResult {
	return &CloudScootGetStatusResult{}
}

var CloudScootGetStatusResult_Success_DEFAULT *JobStatus

func (p *CloudScootGetStatusResult) GetSuccess() *JobStatus {
	if !p.IsSetSuccess() {
		return CloudScootGetStatusResult_Success_DEFAULT
	}
	return p.Success
}

var CloudScootGetStatusResult_Ir_DEFAULT *InvalidRequest

func (p *CloudScootGetStatusResult) GetIr() *InvalidRequest {
	if !p.IsSetIr() {
		return CloudScootGetStatusResult_Ir_DEFAULT
	}
	return p.Ir
}

var CloudScootGetStatusResult_Err_DEFAULT *ScootServerError

func (p *CloudScootGetStatusResult) GetErr() *ScootServerError {
	if !p.IsSetErr() {
		return CloudScootGetStatusResult_Err_DEFAULT
	}
	return p.Err
}
func (p *CloudScootGetStatusResult) IsSetSuccess() bool {
	return p.Success != nil
}

func (p *CloudScootGetStatusResult) IsSetIr() bool {
	return p.Ir != nil
}

func (p *CloudScootGetStatusResult) IsSetErr() bool {
	return p.Err != nil
}

func (p *CloudScootGetStatusResult) Read(iprot thrift.TProtocol) error {
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
		case 0:
			if err := p.readField0(iprot); err != nil {
				return err
			}
		case 1:
			if err := p.readField1(iprot); err != nil {
				return err
			}
		case 2:
			if err := p.readField2(iprot); err != nil {
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

func (p *CloudScootGetStatusResult) readField0(iprot thrift.TProtocol) error {
	p.Success = &JobStatus{}
	if err := p.Success.Read(iprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Success), err)
	}
	return nil
}

func (p *CloudScootGetStatusResult) readField1(iprot thrift.TProtocol) error {
	p.Ir = &InvalidRequest{}
	if err := p.Ir.Read(iprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Ir), err)
	}
	return nil
}

func (p *CloudScootGetStatusResult) readField2(iprot thrift.TProtocol) error {
	p.Err = &ScootServerError{}
	if err := p.Err.Read(iprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Err), err)
	}
	return nil
}

func (p *CloudScootGetStatusResult) Write(oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin("GetStatus_result"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err)
	}
	if err := p.writeField0(oprot); err != nil {
		return err
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

func (p *CloudScootGetStatusResult) writeField0(oprot thrift.TProtocol) (err error) {
	if p.IsSetSuccess() {
		if err := oprot.WriteFieldBegin("success", thrift.STRUCT, 0); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 0:success: ", p), err)
		}
		if err := p.Success.Write(oprot); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Success), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 0:success: ", p), err)
		}
	}
	return err
}

func (p *CloudScootGetStatusResult) writeField1(oprot thrift.TProtocol) (err error) {
	if p.IsSetIr() {
		if err := oprot.WriteFieldBegin("ir", thrift.STRUCT, 1); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:ir: ", p), err)
		}
		if err := p.Ir.Write(oprot); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Ir), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 1:ir: ", p), err)
		}
	}
	return err
}

func (p *CloudScootGetStatusResult) writeField2(oprot thrift.TProtocol) (err error) {
	if p.IsSetErr() {
		if err := oprot.WriteFieldBegin("err", thrift.STRUCT, 2); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 2:err: ", p), err)
		}
		if err := p.Err.Write(oprot); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Err), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 2:err: ", p), err)
		}
	}
	return err
}

func (p *CloudScootGetStatusResult) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("CloudScootGetStatusResult(%+v)", *p)
}

// Attributes:
//  - JobId
type CloudScootKillJobArgs struct {
	JobId string `thrift:"jobId,1" json:"jobId"`
}

func NewCloudScootKillJobArgs() *CloudScootKillJobArgs {
	return &CloudScootKillJobArgs{}
}

func (p *CloudScootKillJobArgs) GetJobId() string {
	return p.JobId
}
func (p *CloudScootKillJobArgs) Read(iprot thrift.TProtocol) error {
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

func (p *CloudScootKillJobArgs) readField1(iprot thrift.TProtocol) error {
	if v, err := iprot.ReadString(); err != nil {
		return thrift.PrependError("error reading field 1: ", err)
	} else {
		p.JobId = v
	}
	return nil
}

func (p *CloudScootKillJobArgs) Write(oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin("KillJob_args"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err)
	}
	if err := p.writeField1(oprot); err != nil {
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

func (p *CloudScootKillJobArgs) writeField1(oprot thrift.TProtocol) (err error) {
	if err := oprot.WriteFieldBegin("jobId", thrift.STRING, 1); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:jobId: ", p), err)
	}
	if err := oprot.WriteString(string(p.JobId)); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T.jobId (1) field write error: ", p), err)
	}
	if err := oprot.WriteFieldEnd(); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write field end error 1:jobId: ", p), err)
	}
	return err
}

func (p *CloudScootKillJobArgs) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("CloudScootKillJobArgs(%+v)", *p)
}

// Attributes:
//  - Success
//  - Ir
//  - Err
type CloudScootKillJobResult struct {
	Success *JobStatus        `thrift:"success,0" json:"success,omitempty"`
	Ir      *InvalidRequest   `thrift:"ir,1" json:"ir,omitempty"`
	Err     *ScootServerError `thrift:"err,2" json:"err,omitempty"`
}

func NewCloudScootKillJobResult() *CloudScootKillJobResult {
	return &CloudScootKillJobResult{}
}

var CloudScootKillJobResult_Success_DEFAULT *JobStatus

func (p *CloudScootKillJobResult) GetSuccess() *JobStatus {
	if !p.IsSetSuccess() {
		return CloudScootKillJobResult_Success_DEFAULT
	}
	return p.Success
}

var CloudScootKillJobResult_Ir_DEFAULT *InvalidRequest

func (p *CloudScootKillJobResult) GetIr() *InvalidRequest {
	if !p.IsSetIr() {
		return CloudScootKillJobResult_Ir_DEFAULT
	}
	return p.Ir
}

var CloudScootKillJobResult_Err_DEFAULT *ScootServerError

func (p *CloudScootKillJobResult) GetErr() *ScootServerError {
	if !p.IsSetErr() {
		return CloudScootKillJobResult_Err_DEFAULT
	}
	return p.Err
}
func (p *CloudScootKillJobResult) IsSetSuccess() bool {
	return p.Success != nil
}

func (p *CloudScootKillJobResult) IsSetIr() bool {
	return p.Ir != nil
}

func (p *CloudScootKillJobResult) IsSetErr() bool {
	return p.Err != nil
}

func (p *CloudScootKillJobResult) Read(iprot thrift.TProtocol) error {
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
		case 0:
			if err := p.readField0(iprot); err != nil {
				return err
			}
		case 1:
			if err := p.readField1(iprot); err != nil {
				return err
			}
		case 2:
			if err := p.readField2(iprot); err != nil {
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

func (p *CloudScootKillJobResult) readField0(iprot thrift.TProtocol) error {
	p.Success = &JobStatus{}
	if err := p.Success.Read(iprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Success), err)
	}
	return nil
}

func (p *CloudScootKillJobResult) readField1(iprot thrift.TProtocol) error {
	p.Ir = &InvalidRequest{}
	if err := p.Ir.Read(iprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Ir), err)
	}
	return nil
}

func (p *CloudScootKillJobResult) readField2(iprot thrift.TProtocol) error {
	p.Err = &ScootServerError{}
	if err := p.Err.Read(iprot); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T error reading struct: ", p.Err), err)
	}
	return nil
}

func (p *CloudScootKillJobResult) Write(oprot thrift.TProtocol) error {
	if err := oprot.WriteStructBegin("KillJob_result"); err != nil {
		return thrift.PrependError(fmt.Sprintf("%T write struct begin error: ", p), err)
	}
	if err := p.writeField0(oprot); err != nil {
		return err
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

func (p *CloudScootKillJobResult) writeField0(oprot thrift.TProtocol) (err error) {
	if p.IsSetSuccess() {
		if err := oprot.WriteFieldBegin("success", thrift.STRUCT, 0); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 0:success: ", p), err)
		}
		if err := p.Success.Write(oprot); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Success), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 0:success: ", p), err)
		}
	}
	return err
}

func (p *CloudScootKillJobResult) writeField1(oprot thrift.TProtocol) (err error) {
	if p.IsSetIr() {
		if err := oprot.WriteFieldBegin("ir", thrift.STRUCT, 1); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 1:ir: ", p), err)
		}
		if err := p.Ir.Write(oprot); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Ir), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 1:ir: ", p), err)
		}
	}
	return err
}

func (p *CloudScootKillJobResult) writeField2(oprot thrift.TProtocol) (err error) {
	if p.IsSetErr() {
		if err := oprot.WriteFieldBegin("err", thrift.STRUCT, 2); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field begin error 2:err: ", p), err)
		}
		if err := p.Err.Write(oprot); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T error writing struct: ", p.Err), err)
		}
		if err := oprot.WriteFieldEnd(); err != nil {
			return thrift.PrependError(fmt.Sprintf("%T write field end error 2:err: ", p), err)
		}
	}
	return err
}

func (p *CloudScootKillJobResult) String() string {
	if p == nil {
		return "<nil>"
	}
	return fmt.Sprintf("CloudScootKillJobResult(%+v)", *p)
}
