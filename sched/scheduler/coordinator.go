package scheduler

import (
	"fmt"
	"sync"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/queue"
	"github.com/scootdev/scoot/sched/worker"
	"github.com/scootdev/scoot/workerapi"
)

type coordinator struct {
	queue         queue.Queue
	sc            saga.SagaCoordinator
	workerFactory worker.WorkerFactory

	cluster chan []cluster.NodeUpdate
	queueCh chan queue.WorkItem
	replyCh chan reply

	workers    map[cluster.NodeId]workerapi.Worker
	queueItems map[string]queue.WorkItem
	sagas      map[string]chan saga.Message

	st *schedulerState

	// Writers' count == the number of in-flight writers, i.e.:
	// active cluster, active queue, or in-flight rpc
	writers *sync.WaitGroup

	errCh chan error

	actives *jobStates
}

func (c *coordinator) Sagas() saga.SagaCoordinator {
	return c.sc
}

func (c *coordinator) Wait() error {
	return <-c.errCh
}

func (c *coordinator) closeWhenDone() {
	c.writers.Wait()
	close(c.replyCh)
}

func (c *coordinator) done() bool {
	return c.cluster == nil && c.queueCh == nil && c.replyCh == nil
}

func (c *coordinator) loop() {
	for !c.done() {
		select {
		case updates, ok := <-c.cluster:
			if !ok {
				c.cluster = nil
				c.writers.Done()
				continue
			}
			// log.Println("Cluster Updates")
			for _, update := range updates {
				c.handleNodeUpdate(update)
			}
		case item, ok := <-c.queueCh:
			if !ok {
				c.queueCh = nil
				c.writers.Done()
				continue
			}
			c.handleWorkItem(item)
		case reply, ok := <-c.replyCh:
			if !ok {
				c.replyCh = nil
				continue
			}
			c.writers.Done()
			switch reply := reply.(type) {
			case queueReply:
				if reply.err != nil && c.st.err != nil {
					c.st.err = nil
				}
			case sagaLogReply:
				c.handleSagaLogReply(reply)
			case workerReply:
				// log.Println("Worker Reply")
				reply(c.st)
			default:
				p("Unexpected reply type: %v %T", reply, reply)
			}
		}
		c.tick()
	}
	for _, w := range c.workers {
		w.Close()
	}

	for _, sCh := range c.sagas {
		close(sCh)
	}
	c.errCh <- c.st.err
}

func (c *coordinator) handleSagaLogReply(r sagaLogReply) {
	// log.Println("Saga Reply")
	if r.err != nil && c.st.err == nil {
		c.st.err = r.err
	}
	if r.err == nil {
		j := c.st.getJob(r.id)
		if j != nil && j.status == jobNew {
			j.status = jobPersisted
		}
	}
}

func (c *coordinator) tick() {
	if c.cluster == nil && c.queueCh == nil {
		// Our inputs are closed; we don't want to plan anything more,
		// just let our in-flight rpcs drain
		return
	}
	// log.Println("State Before", render.Render(c.st))
	p := makePlanner(c.st)
	p.plan()
	// log.Println("State After", render.Render(c.st))
	// log.Println("Actions", render.Render(p.actions))
	for _, rpc := range p.rpcs {
		c.dispatchRpc(rpc)
	}
}

func (c *coordinator) dispatchRpc(rpc rpc) {
	// log.Println("Dispatching RPC:", render.Render(rpc))
	c.writers.Add(1)
	switch rpc := rpc.(type) {
	case logSagaMsg:
		sagaCh := c.sagas[rpc.sagaId]
		sagaCh <- rpc.msg
		if rpc.final {
			close(sagaCh)
			delete(c.sagas, rpc.sagaId)
		}
	case dequeueWorkItem:
		item := c.queueItems[rpc.jobId]
		delete(c.queueItems, rpc.jobId)
		go func() {
			// log.Println("Replying dequeue", item, c.replyCh)
			c.replyCh <- queueReply{item.Dequeue()}
		}()
	case workerRpc:
		worker := c.workers[cluster.NodeId(rpc.workerId)]
		go func() {
			// log.Println("Replying Worker RPC")
			c.replyCh <- rpc.call(worker)
		}()
	default:
		p("Unexpected rpc: %v %T", rpc, rpc)
	}
}

func (c *coordinator) handleNodeUpdate(update cluster.NodeUpdate) {
	// log.Println("Handling Node Update", update, update.UpdateType == cluster.NodeAdded)
	switch update.UpdateType {
	case cluster.NodeAdded:
		_, ok := c.workers[update.Id]
		if !ok {
			c.workers[update.Id] = c.workerFactory(update.Node)
		}

		for _, ws := range c.st.workers {
			if ws.id == string(update.Id) {
				// We already have this worker
				return
			}
		}
		c.st.workers = append(c.st.workers,
			&workerState{
				id:     string(update.Id),
				status: workerAdded,
			})
	case cluster.NodeRemoved:
		worker, ok := c.workers[update.Id]
		if ok {
			worker.Close()
			delete(c.workers, update.Id)
		}

		for i, ws := range c.st.workers {
			if ws.id == string(update.Id) {
				c.st.workers = append(c.st.workers[0:i], c.st.workers[i:]...)
				continue
			}
		}
	default:
		p("Unexpected NodeUpdateType: %v", update.UpdateType)
	}
}

func (c *coordinator) handleWorkItem(item queue.WorkItem) {
	jobId := item.Job().Id
	c.queueItems[jobId] = item
	// log.Println("Work Item", jobId, item, c.queueItems)

	c.st.incoming = append(c.st.incoming, item.Job())

	// This job isn't yet started, so don't write anything, but begin listening for saga writes for this job.
	s := c.sc.MakeEmptySaga(jobId)
	bufCh := make(chan saga.Message)
	go sagaBufferLoop(jobId, c.actives, s, bufCh, c.replyCh)
	c.sagas[jobId] = bufCh
}

func sagaBufferLoop(sagaId string, actives *jobStates, s *saga.Saga, bufCh chan saga.Message, replyCh chan reply) {
	var pending []saga.Message
	var inflight chan error
	for bufCh != nil || len(pending) > 0 {
		select {
		case msg, ok := <-bufCh:
			if !ok {
				bufCh = nil
				continue
			}
			pending = append(pending, msg)
		case err := <-inflight:
			pending = pending[1:]
			inflight = nil
			if err != nil {
				// We have hit an error. Stop trying to write
				// (it might make the situation worse)
				// Just exhaust the buffer so the coordinator doesn't get stuck
				go exhaustBufferLoop(sagaId, bufCh, replyCh)
				return
			}
		}
		if inflight == nil && len(pending) > 0 {
			inflight = make(chan error)
			msg := pending[0]
			go func() {
				err := s.LogMessage(msg)
				actives.setState(sagaId, s.GetState())
				inflight <- err
				replyCh <- sagaLogReply{sagaId, err}

			}()
		}
	}
}

func exhaustBufferLoop(sagaId string, bufCh chan saga.Message, replyCh chan reply) {
	for _ = range bufCh {
		replyCh <- sagaLogReply{sagaId, fmt.Errorf("Stopped Logging")}
	}
}
