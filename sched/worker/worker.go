package worker

import (
	"errors"
	"time"

	"github.com/luci/go-render/render"
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/workerapi"
)

// Create a Worker Client that talks to node
type WorkerFactory func(node cluster.Node) workerapi.Worker

// TODO(dbentley): delete this function
// Instead, the scheduler should just use Worker above
func RunAndWait(cmd *runner.Command, w workerapi.Worker) error {
	status, err := w.Run(cmd)
	if err != nil {
		return err
	}
	for !status.State.IsDone() {
		time.Sleep(250 * time.Millisecond) //TODO: make configurable
		workerStatus, err := w.Status()
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
