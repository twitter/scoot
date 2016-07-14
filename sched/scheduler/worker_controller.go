package scheduler

import (
	"github.com/scootdev/scoot/sched"
	cm "github.com/scootdev/scoot/sched/clustermembership"
	"github.com/scootdev/scoot/sched/worker"
	"github.com/scootdev/scoot/sched/worker/rpc"
)

type workerController struct {
	worker worker.Worker
}

func NewWorkerController(node cm.Node) *workerController {
	worker := rpc.NewThriftWorker(node)
}

func (c *workerController) Run(task sched.TaskDefinition) error {
	cmd := translateToWorker(task)
	runId, err := worker.Run(cmd)
	if err != nil {
		return err
	}
	status, error := worker.Wait(runId)
	if err != nil {
		return err
	}
	// TODO: return status somehow
}
