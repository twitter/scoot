package rpc

import (
	"errors"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/luci/go-render/render"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/worker"
	apiclient "github.com/scootdev/scoot/workerapi/client"
)

type thriftWorker struct {
	client apiclient.Client
}

func NewThriftWorker(
	transportFactory thrift.TTransportFactory,
	protocolFactory thrift.TProtocolFactory,
	addr string,
) worker.Worker {
	return &thriftWorker{apiclient.NewClient(transportFactory, protocolFactory, addr)}
}

func (c *thriftWorker) RunAndWait(task sched.TaskDefinition) error {
	status, err := c.client.Run(&task.Command)
	if err != nil {
		return err
	}
	for !status.State.IsDone() {
		time.Sleep(250 * time.Millisecond) //TODO: make configurable
		workerStatus, err := c.client.QueryWorker()
		updated := false
		if err != nil {
			return err
		}
		if workerStatus.Error != nil {
			return workerStatus.Error
		}
		for _, s := range workerStatus.Runs {
			if s.RunId == status.RunId {
				status = s
				updated = true
				break
			}
		}
		if !updated {
			return errors.New("RunId disappeared!")
		}
	}
	if status.State != runner.COMPLETE || status.ExitCode != 0 {
		return errors.New(render.Render(status))
	}
	return nil
}
