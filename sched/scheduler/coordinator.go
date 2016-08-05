package scheduler

import (
	"sync"
	"time"

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

	workers      map[cluster.NodeId]workerapi.Worker
	queueItems   map[string]queue.WorkItem
	sagas        map[string]chan saga.Message
	inflightRpcs int

	st *schedulerState

	err     error
	errWg   *sync.WaitGroup
	loopWg  *sync.WaitGroup
	actives *jobStates
	l       listener
}

func (c *coordinator) Sagas() saga.SagaCoordinator {
	return c.sc
}

func (c *coordinator) WaitForError() error {
	c.errWg.Wait()
	return c.err
}

func (c *coordinator) WaitForShutdown() {
	c.loopWg.Wait()
}

func (c *coordinator) done() bool {
	return c.cluster == nil && c.queueCh == nil && c.inflightRpcs == 0
}

func (c *coordinator) loop() {
	for !c.done() {
		c.update()
		c.tick()
	}
	c.shutdown()
}

func (c *coordinator) tick() {
	if c.cluster == nil && c.queueCh == nil {
		// Our inputs are closed; we don't want to plan anything more,
		// just let our in-flight rpcs drain
		return
	}

	// TODO(dbentley): use a fake clock
	c.st.now = time.Now().UTC()

	c.l.BeforeState(c.st)
	p := makePlanner(c.st)
	p.plan()
	c.l.AfterState(c.st)
	c.l.Actions(p.actions)
	c.l.Rpcs(p.rpcs)
	for _, rpc := range p.rpcs {
		c.dispatchRpc(rpc)
	}
}

func (c *coordinator) update() {
	c.l.Select()

	select {
	case <-time.After(c.st.nextTimeout().Sub(time.Now().UTC())):
		c.l.Timeout()
	case updates, ok := <-c.cluster:
		if !ok {
			c.cluster = nil
			continue
		}
		c.l.ClusterUpdate(updates)
		for _, update := range updates {
			c.handleNodeUpdate(update)
		}
	case item, ok := <-c.queueCh:
		if !ok {
			c.queueCh = nil
			continue
		}
		c.l.QueueUpdate(item)
		c.handleWorkItem(item)
	case reply := <-c.replyCh:
		c.inflightRpcs--
		switch reply := reply.(type) {
		case queueReply:
			c.l.QueueReply(reply)
			c.error(reply.err)
		case sagaLogReply:
			c.l.SagaLogReply(reply)
			c.error(r.err)
			if j := c.st.getJob(r.id); j != nil {
				if j.status == jobNew {
					if r.err == nil {
						j.status = jobPersisted
					} else {
						j.status = jobCannotPersist
					}
				}
			}
		case workerReply:
			c.l.WorkerReply(reply)
			reply.apply(c.st)
		default:
			p("Unexpected reply type: %v %T", reply, reply)
		}
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

	c.st.incoming = append(c.st.incoming, item.Job())

	// This job isn't yet started, so don't write anything, but begin listening for saga writes for this job.
	s := c.sc.MakeEmptySaga(jobId)
	bufCh := make(chan saga.Message)
	writeCh := make(chan saga.Message)
	go sagaBufferLoop(bufCh, writeCh)
	go sagaWriteLoop(jobId, c.actives, s, writeCh, c.replyCh)
	c.sagas[jobId] = bufCh
}

func (c *coordinator) dispatchRpc(rpc rpc) {
	c.inflightRpcs++
	switch rpc := rpc.(type) {
	case logSagaMsg:
		sagaCh := c.sagas[rpc.sagaId]
		sagaCh <- rpc.msg
		if rpc.final {
			close(sagaCh)
			delete(c.sagas, rpc.sagaId)
		}
	case respondWorkItem:
		item := c.queueItems[rpc.jobId]
		delete(c.queueItems, rpc.jobId)
		go func() {
			c.replyCh <- queueReply{item.Respond(rpc.claimed)}
		}()
	case workerRpc:
		worker := c.workers[cluster.NodeId(rpc.workerId())]
		go func() {
			c.replyCh <- rpc.call(worker)
		}()
	default:
		p("Unexpected rpc: %v %T", rpc, rpc)
	}
}

func sagaBufferLoop(inCh chan saga.Message, outCh chan saga.Message) {
	var pending []saga.Message
	for inCh != nil || len(pending) > 0 {
		var sendCh chan saga.Message
		var sendMsg saga.Message
		if len(pending) > 0 {
			sendCh, sendMsg = outCh, pending[0]
		}
		select {
		case msg, ok := <-inCh:
			if !ok {
				inCh = nil
				continue
			}
			pending = append(pending, msg)
		case sendCh <- sendMsg:
			pending = pending[1:]
		}
	}
}

func sagaWriteLoop(sagaId string, actives *jobStates, s *saga.Saga, inCh chan saga.Message, replyCh chan reply) {
	var err error
	for msg := range inCh {
		// Don't write if we've already hit an error
		if err == nil {
			err = s.LogMessage(msg)
			actives.setState(sagaId, s.GetState())
		}
		replyCh <- sagaLogReply{sagaId, err}
	}
}

func (c *coordinator) shutdown() {
	// Shut down
	for _, w := range c.workers {
		w.Close()
	}
	for _, sCh := range c.sagas {
		close(sCh)
	}
	if c.err == nil {
		// Finished without hitting an error
		c.errWg.Done()
	}
	c.loopWg.Done()
}

// Note an unrecoverable error
func (c *coordinator) error(err error) {
	if c.err == nil && err != nil {
		c.err = err
		c.errWg.Done()
	}
}
