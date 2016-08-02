package scheduler

import (
	"sync"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/queue"
)

type coordinator struct {
	cluster  chan []cluster.NodeUpdate
	queue    chan queue.WorkItem
	updateCh chan interface{}

	workers    map[cluster.NodeId]chan workerRpc
	queueItems map[string]queue.WorkIem
	sagas      map[string]chan sagaRpc

	st *schedulerState

	// Writers' count == the number of in-flight writers, i.e.:
	// active cluster, active queue, or in-flight rpc
	writers *sync.WaitGroup
}

func (c *coordinator) closeWhenDone() {
	c.writers.Wait()
	close(c.inCh)
}

func (c *coordinator) loop() error {
	for {
		select {
		case updates, ok := <-c.cluster:
			if !ok {
				c.cluster = nil
				c.writers.Done()
				continue
			}
			for _, update := range updates {
				c.handleNodeUpdates(update)
			}
		case item, ok := <-c.queue:
			if !ok {
				c.queue = nil
				c.writers.Done()
				continue
			}
			c.handleWorkItem(item)
		case update, ok := c.updateCh:
			if !ok {
				break
			}
			update.apply(c.st)
			c.writers.Done()
		}
		c.tick()
	}
	for _, wCh := range c.workers {
		close(wCh)
	}

	for _, sCh := range c.sagas {
		close(sCh)
	}
	return c.st.error
}

func (c *coordinator) tick() {
	p := makePlanner(st)
	p.plan()
	rpcs := p.rpcs()
	for _, rpc := range rpcs {
		c.dispatchRpc(rpc)
	}
}

func (c *coordinator) dispatchRpc(rpc rpc) {
	switch rpc := rpc.(type) {
	case sagaRpc:
		sagaCh := c.sagas[rpc.sagaId]
		c.writers.Add(1)
		sagaCh <- rpc
		if rpc.final {
			close(sagaCh)
			delete(c.sagas, rpc.sagaId)
		}
	case queueRpc:
		item := c.queueItems[rpc.JobId]
		delete(c.queueItems, rpc.jobId)
		c.writers.Add(1)
		go func() {
			c.updateCh <- errorUpdate(item.Dequeue())
		}()
	case workerRpc:
		workerCh := c.sagas[rpc.workerId]
		c.writers.Add(1)
		workerCh <- rpc
	default:
		p("Unexpected rpc: %v %T", rpc, rpc)
	}
}

func (c *coordinator) handleNodeUpdate(update []cluster.NodeUpdate) {
	switch update.UpdateType {
	case cluster.NodeAdded:
		_, ok := c.workers[update.Id]
		if !ok {
			workerCh := make(chan workerRpc)
			go workerRpcLoop(string(update.Id), workerCh, c.updateCh)
			c.workers[update.Id] = workerCh
		}

		for i, ws := range c.st.workers {
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
		workerCh, ok := c.workers[update.Id]
		if ok {
			close(workerCh)
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

func (c *coordinator) handleWorkeItem(item queue.WorkItem) {
	jobId := item.Job().Id
	c.queueItems[jobId] = item

	c.st.incoming = append(c.st.incoming, item.Job())

	// This job isn't yet started, so don't write anything, but begin listening for saga writes for this job.
	sagaCh := make(chan sagaRpc)
	s := c.sc.MakeEmptySaga(jobId)
	bufCh := make(chan saga.SagaMsg)
	writeCh := make(chan saga.SagaMsg)
	go sagaBufferLoop(bufCh, doCh)
	go sagaWriteLoop(s, writeCh, c.updateCh)
	c.sagas[jobId] = bufCh
}

func sagaWriteLoop(s *saga.Saga, writeCh chan saga.SagaMsg, updateCh chan update) {
	for msg := range writeCh {
		updateCh <- &errorUpdate{s.LogMessage(msg)}
	}
}

func sagaBufferLoop(bufCh chan saga.SagaMsg, writeCh chan saga.SagaMsg) {
	var pending []saga.SagaMsg
	for bufCh != nil || len(pending) > 0 {
		if len(pending) == 0 {
			msg, ok := <-bufCh
			if !ok {
				bufCh = nil
				continue
			}
			pending = append(pending, msg)
		} else {
			select {
			case msg, ok := <-bufCh:
				if !ok {
					bufCh = nil
					continue
				}
				pending = append(pending, msg)
			case writeChCh <- pending[0]:
				pending = pending[1:]
			}
		}
	}
	// 	var sendCh chan saga.SagaMsg
	// 	var first saga.SagaMsg
	// 	if len(pending > 0) {
	// 		sendCh := writeCh
	// 		first := pending[0]
	// 	}
	// 	select {
	// 	case msg, ok := <-bufCh:
	// 		if !ok {
	// 			bufCh = nil
	// 			continue
	// 		}
	// 		pending = append(pending, msg)
	// 	case sendCh <- first:
	// 		pending = pending[1:]
	// 	}
	// }
	close(writeCh)
}
