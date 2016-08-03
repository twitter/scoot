package scheduler

import (
	"sync"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/queue"
	"github.com/scootdev/scoot/sched/worker"
)

type coordinator struct {
	cluster chan []cluster.NodeUpdate
	queue   chan queue.WorkItem
	replyCh chan reply

	sc            saga.SagaCoordinator
	workerFactory worker.WorkerFactory

	workers    map[cluster.NodeId]worker.Worker
	queueItems map[string]queue.WorkItem
	sagas      map[string]chan saga.Message

	st *schedulerState

	// Writers' count == the number of in-flight writers, i.e.:
	// active cluster, active queue, or in-flight rpc
	writers *sync.WaitGroup

	errCh chan error
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

func (c *coordinator) loop() {
	for {
		select {
		case updates, ok := <-c.cluster:
			if !ok {
				c.cluster = nil
				c.writers.Done()
				continue
			}
			for _, update := range updates {
				c.handleNodeUpdate(update)
			}
		case item, ok := <-c.queue:
			if !ok {
				c.queue = nil
				c.writers.Done()
				continue
			}
			c.handleWorkItem(item)
		case reply, ok := <-c.replyCh:
			if !ok {
				break
			}
			c.writers.Done()
			switch reply := reply.(type) {
			case *errorReply:
				if reply.err == nil {
					// We don't need to schedule if we didn't change anything
					continue
				}
				if c.st.err == nil {
					c.st.err = reply.err
				}
			case workerReply:
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

func (c *coordinator) tick() {
	if c.cluster == nil && c.queue == nil {
		// Our inputs are closed; we don't want to plan anything more,
		// just let our in-flight rpcs drain
		return
	}
	p := makePlanner(c.st)
	p.plan()
	rpcs := p.rpcs()
	for _, rpc := range rpcs {
		c.dispatchRpc(rpc)
	}
}

func (c *coordinator) dispatchRpc(rpc rpc) {
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
			c.replyCh <- &errorReply{item.Dequeue()}
		}()
	case workerRpc:
		worker := c.workers[cluster.NodeId(rpc.workerId)]
		go func() {
			c.replyCh <- rpc.call(worker)
		}()
	default:
		p("Unexpected rpc: %v %T", rpc, rpc)
	}
}

func (c *coordinator) handleNodeUpdate(update cluster.NodeUpdate) {
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
			c.st.workers = append(c.st.workers,
				&workerState{
					id:     string(update.Id),
					status: workerAdded,
				})
		}
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

	c.st.incoming = append(c.st.incoming, item.Job())

	// This job isn't yet started, so don't write anything, but begin listening for saga writes for this job.
	s := c.sc.MakeEmptySaga(jobId)
	bufCh := make(chan saga.Message)
	go sagaBufferLoop(s, bufCh, c.replyCh)
	c.sagas[jobId] = bufCh
}

func sagaBufferLoop(s *saga.Saga, bufCh chan saga.Message, replyCh chan reply) {
	var pending []saga.Message
	var inflight chan struct{}
	for bufCh != nil || len(pending) > 0 {
		select {
		case msg, ok := <-bufCh:
			if !ok {
				bufCh = nil
				continue
			}
			pending = append(pending, msg)
		case <-inflight:
			pending = pending[1:]
			inflight = nil
		}
		if inflight == nil && len(pending) > 0 {
			inflight = make(chan struct{})
			msg := pending[0]
			go func() {
				defer close(inflight)
				replyCh <- &errorReply{s.LogMessage(msg)}
			}()
		}
	}
}
