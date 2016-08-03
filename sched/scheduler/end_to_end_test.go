package scheduler_test

import (
	"sync"
	"testing"

	"github.com/scootdev/scoot/cloud/cluster"
	memcluster "github.com/scootdev/scoot/cloud/cluster/memory"
	"github.com/scootdev/scoot/runner"
	fakeexecer "github.com/scootdev/scoot/runner/execer/fake"
	localrunner "github.com/scootdev/scoot/runner/local"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/queue"
	memqueue "github.com/scootdev/scoot/sched/queue/memory"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/workerapi"
	runnerworker "github.com/scootdev/scoot/workerapi/runner"
)

func TestSimple(t *testing.T) {
	h := makeHelper(t)
	h.addNodes("worker1")
	defer h.close()
	j := h.makeJob()
	h.runAndWait(j, sched.Completed)
}

type helper struct {
	t   *testing.T
	q   queue.Queue
	cl  chan []cluster.NodeUpdate
	log saga.SagaLog

	wgs map[string]*sync.WaitGroup

	s scheduler.Scheduler
}

func (h *helper) runAndWait(j sched.JobDefinition, waitFor sched.Status) string {
	id, err := h.s.Run(j)
	if err != nil {
		h.t.Fatalf("Could not run job: %v", err)
	}
	for {
		status, err := h.s.GetStatus(id)
		if err != nil {
			h.t.Fatalf("Error getting status: %v", err)
		}
		if status.Status == waitFor {
			break
		}
	}
	return id
}

func (h *helper) close() error {
	close(h.cl)
	h.q.Close()
	if err := h.s.Wait(); err != nil {
		h.t.Fatalf("Error running: %v", err)
	}
	return nil
}

func (h *helper) makeJob() sched.JobDefinition {
	return sched.JobDefinition{
		Tasks: map[string]sched.TaskDefinition{
			"task1": {
				Command: runner.Command{
					Argv: []string{"complete 0"},
				},
			},
		},
	}
}

func (h *helper) makeWorker(node cluster.Node) workerapi.Worker {
	wg := &sync.WaitGroup{}
	h.wgs[string(node.Id())] = wg
	exec := fakeexecer.NewSimExecer(wg)
	r := localrunner.NewSimpleRunner(exec)
	return runnerworker.MakeWorker(r)
}

func (h *helper) addNodes(hostnames ...string) {
	updates := []cluster.NodeUpdate{}
	for _, hostname := range hostnames {
		updates = append(updates, cluster.NewAdd(memcluster.NewIdNode(hostname)))
	}
	h.cl <- updates
}

func makeHelper(t *testing.T) *helper {
	h := &helper{
		t:   t,
		q:   memqueue.NewSimpleQueue(1),
		cl:  make(chan []cluster.NodeUpdate),
		log: saga.MakeInMemorySagaLog(),
		wgs: make(map[string]*sync.WaitGroup),
	}
	h.s = scheduler.NewScheduler(h.cl, h.q, h.log, h.makeWorker)
	return h
}
