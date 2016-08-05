package scheduler

import (
	"log"

	"github.com/luci/go-render/render"
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/sched/queue"
)

type listener interface {
	// Begin Loop
	Select()

	// Updates
	ClusterUpdate([]cluster.NodeUpdate)
	QueueUpdate(queue.WorkItem)
	Timeout()

	// Replies
	SagaLogReply(sagaLogReply)
	QueueReply(queueReply)
	WorkerReply(workerReply)

	// Plan
	BeforeState(*schedulerState)
	AfterState(*schedulerState)
	Actions([]action)
	Rpcs([]rpc)
}

type noopListener struct{}

func (l *noopListener) Select()                                    {}
func (l *noopListener) ClusterUpdate(updates []cluster.NodeUpdate) {}
func (l *noopListener) QueueUpdate(item queue.WorkItem)            {}
func (l *noopListener) Timeout()                                   {}
func (l *noopListener) SagaLogReply(r sagaLogReply)                {}
func (l *noopListener) QueueReply(r queueReply)                    {}
func (l *noopListener) WorkerReply(r workerReply)                  {}
func (l *noopListener) BeforeState(s *schedulerState)              {}
func (l *noopListener) AfterState(s *schedulerState)               {}
func (l *noopListener) Actions(actions []action)                   {}
func (l *noopListener) Rpcs(rpcs []rpc)                            {}

type loggingListener struct{}

func (l *loggingListener) Select() {
	log.Println("Select")
}

func (l *loggingListener) ClusterUpdate(updates []cluster.NodeUpdate) {
	log.Println("Cluster Updates", render.Render(updates))
}

func (l *loggingListener) Timeout() {
	log.Println("Timeout Update")
}

func (l *loggingListener) QueueUpdate(item queue.WorkItem) {
	log.Println("Queue Update", item.Job().Id)
}

func (l *loggingListener) SagaLogReply(r sagaLogReply) {
	log.Println("SagaLog Reply", render.Render(r))
}

func (l *loggingListener) QueueReply(r queueReply) {
	log.Println("Queue Reply", render.Render(r))
}

func (l *loggingListener) WorkerReply(r workerReply) {
	log.Println("Worker Reply", render.Render(r))
}

func (l *loggingListener) BeforeState(s *schedulerState) {
	log.Println("Before State", render.Render(s))
}

func (l *loggingListener) AfterState(s *schedulerState) {
	log.Println("After State", render.Render(s))
}

func (l *loggingListener) Actions(actions []action) {
	log.Println("Actions", render.Render(actions))
}

func (l *loggingListener) Rpcs(rpcs []rpc) {
	log.Println("Rpcs", render.Render(rpcs))
}
