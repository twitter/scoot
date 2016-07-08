package server

import (
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/queue"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"log"
	"time"
)

func Serve(handler scoot.Proc, addr string, transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory) error {
	transport, err := thrift.NewTServerSocket(addr)
	if err != nil {
		return err
	}
	processor := scoot.NewProcProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	fmt.Println("About to serve")

	return server.Serve()
}

type Handler struct {
	queue queue.Queue
}

func NewHandler(queue queue.Queue) scoot.Proc {
	return &Handler{queue}
}

func (h *Handler) RunJob(def *scoot.JobDefinition) (*scoot.JobId, error) {
	log.Println("Running job", def)

	job, err := thriftJobToScoot(def)
	if err != nil {
		return nil, err
	}

	id, err := h.queue.Enqueue(job)

	if err != nil {
		switch err := err.(type) {
		case *queue.InvalidJobRequest:
			r := scoot.NewInvalidRequest()
			r.Message = new(string)
			*r.Message = err.Error()
			return nil, r
		case *queue.CanNotScheduleNow:
			r := scoot.NewCanNotScheduleNow()
			r.RetryAfterMs = new(int64)
			*r.RetryAfterMs = int64(err.UntilRetry / time.Millisecond)
			return nil, r
		}
		return nil, err
	}

	r := scoot.NewJobId()
	r.ID = id
	return r, nil
}

// Translates thrift job definition message to scoot domain object
func thriftJobToScoot(def *scoot.JobDefinition) (result sched.Job, err error) {
	if def == nil {
		return result, fmt.Errorf("nil job definition")
	}

	for _, t := range def.Tasks {
		var task sched.Task
		if t == nil {
			return result, fmt.Errorf("nil task definition")
		}
		task.Id = *t.ID
		if t.Command == nil {
			return result, fmt.Errorf("nil command")
		}
		task.Command = t.Command.Argv
		if t.SnapshotId != nil {
			task.SnapshotId = *t.SnapshotId
		}
		result.Tasks = append(result.Tasks, task)
	}

	if def.JobType != nil {
		result.JobType = def.JobType.String()
	}

	return result, nil
}
