package scheduler

import (
	"reflect"
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

func TestWritingStartSagaFails(t *testing.T) {
	h := makeHelper(t)
	h.addNodes("worker1")
	defer h.close()
	j := h.makeJob()
	err := saga.NewInternalLogError("test error")
	h.chaos.SetLogError(err)
	h.run(j)
	h.assertError(err)
}

type helper struct {
	t   *testing.T
	q   queue.Queue
	cl  chan []cluster.NodeUpdate
	log saga.SagaLog

	err error

	chaos *chaosController

	wgs map[string]*sync.WaitGroup

	s Scheduler
}

func (h *helper) run(j sched.JobDefinition) string {
	id, err := h.s.Run(j)
	if err != nil {
		h.t.Fatalf("Could not run job: %v", err)
	}
	return id
}

func (h *helper) runAndWait(j sched.JobDefinition, waitFor sched.Status) string {
	id := h.run(j)
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

func (h *helper) assertError(expected error) {
	h.err = expected
	actual := h.s.WaitForError()
	if !reflect.DeepEqual(actual, expected) {
		h.t.Fatalf("Unequal errors: %v %T %v %T", actual, actual, expected, expected)
	}
}

func (h *helper) close() error {
	close(h.cl)
	h.q.Close()

	if h.err == nil {
		err := h.s.WaitForError()
		if err != nil {
			h.t.Fatalf("Unexpected error: %v", err)
		}
	}
	h.s.WaitForShutdown()
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

	h.chaos = &chaosController{
		log: &interceptorLog{Err: nil, Del: h.log},
	}

	h.s = NewScheduler(h.cl, h.q, h.chaos.log, h.makeWorker)
	return h
}
