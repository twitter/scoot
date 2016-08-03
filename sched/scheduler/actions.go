package scheduler

import (
	"fmt"

	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/sched/worker"
)

type pingWorkerAction struct {
	id string
}

func (a *pingWorkerAction) apply(st *schedulerState) []rpc {
	st.getWorker(a.id).status = workerPinged
	return []rpc{workerRpc{a.id, a.queryWorker}}
}

func (a *pingWorkerAction) queryWorker(w worker.Worker) workerReply {
	status, err := w.Query()

	return func(st *schedulerState) {
		st.updateWorker(a, status, err)
	}
}

type startJobAction struct {
	id string
}

func (a *startJobAction) apply(st *schedulerState) []rpc {
	var job sched.Job
	for i, j := range st.incoming {
		if j.Id == a.id {
			job = j
			st.incoming = append(st.incoming[0:i], st.incoming[i:]...)
		}
		break
	}
	if job.Id == "" {
		p("no such job: %v %v", st, a.id)
	}

	st.jobs = append(st.jobs, initialJobState(job))
	// TODO: need to serialize job into binary and pass in here
	// so we can recover the job in case of failure
	return []rpc{logSagaMsg{a.id, saga.MakeStartSagaMessage(a.id, []byte{}), false}}
}

type startRunAction struct {
	jobId    string
	taskId   string
	workerId string
}

func (a *startRunAction) apply(st *schedulerState) []rpc {
	t := st.getTask(a.jobId, a.taskId)
	cmd := &t.def.Command
	t.status = taskRunning
	t.runningOn = a.workerId
	w := st.getWorker(a.workerId)
	w.status = workerBusy
	return []rpc{
		logSagaMsg{a.jobId, saga.MakeStartTaskMessage(a.jobId, a.taskId, []byte(a.workerId)), false},
		workerRpc{a.workerId, func(w worker.Worker) workerReply {
			err := worker.RunAndWait(cmd, w)
			return func(st *schedulerState) {
				st.markRunComplete(a, err)
			}
		}},
	}
}

type endTaskAction struct {
	jobId  string
	taskId string
}

func (a *endTaskAction) apply(st *schedulerState) []rpc {
	t := st.getTask(a.jobId, a.taskId)
	t.status = taskDone
	t.runningOn = ""
	return []rpc{logSagaMsg{a.jobId, saga.MakeEndTaskMessage(a.jobId, a.taskId, []byte{}), false}}
}

type endJobAction struct {
	jobId string
}

func (a *endJobAction) apply(st *schedulerState) []rpc {
	t := st.getJob(a.jobId)
	t.status = jobDone
	return []rpc{logSagaMsg{a.jobId, saga.MakeEndSagaMessage(a.jobId), true}}
}

func p(s string, args ...interface{}) {
	panic(fmt.Errorf(s, args...))
}
