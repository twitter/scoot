package scheduler

import (
	"fmt"
	"time"

	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/workerapi"
)

type pingWorkerAction struct {
	id string
}

func (a *pingWorkerAction) apply(st *schedulerState) []rpc {
	st.getWorker(a.id).lastSend = st.now
	return []rpc{&pingRpc{a: a, sent: st.now}}
}

type pingRpc struct {
	a      *pingWorkerAction
	sent   time.Time
	status *workerapi.WorkerStatus
	err    error
}

func (r *pingRpc) workerId() string { return r.a.id }
func (r *pingRpc) rpc()             {}
func (r *pingRpc) reply()           {}

func (r *pingRpc) call(w workerapi.Worker) workerReply {
	r.status, r.err = w.Status()
	return r
}

func (r *pingRpc) apply(st *schedulerState) {
	ws := st.getWorker(r.a.id)
	if ws == nil {
		return
	}
	if r.err != nil {
		ws.status = workerDown
		return
	}
	if ws.lastRecv.After(r.sent) {
		// Our ping is stale; don't update
		return
	}
	ws.runs = r.status.Runs
	ws.status = workerAvailable
	for _, rs := range ws.runs {
		if rs.State.IsBusy() {
			// HMM(dbentley):
			// if ws.status is just a function of ws.runs, maybe just have it be a method...
			ws.status = workerBusy
		}
	}
	ws.lastRecv = r.sent
}

type startJobAction struct {
	id string
}

func (a *startJobAction) apply(st *schedulerState) []rpc {
	var job sched.Job
	for i, j := range st.incoming {
		if j.Id == a.id {
			job = j
			st.incoming = append(st.incoming[0:i], st.incoming[i+1:]...)
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

type runRpc struct {
	a     *startRunAction
	cmd   *runner.Command
	runId runner.RunId
	err   error
}

func (r *runRpc) call(w workerapi.Worker) workerReply {
	status, err := w.Run(r.cmd)
	r.err = err
	r.runId = status.RunId
	return r
}

func (r *runRpc) workerId() string { return r.a.workerId }
func (r *runRpc) rpc()             {}
func (r *runRpc) reply()           {}

func (r *runRpc) apply(st *schedulerState) {
	t := st.getTask(r.a.jobId, r.a.taskId)
	if t == nil || t.runningOn != r.a.workerId || t.runningAs != runner.RunId("") {
		// The task doesn't need us anymore
		return
	}
	t.runningAs = r.runId
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
		&runRpc{a: a, cmd: cmd},
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
	t.runningAs = runner.RunId("")
	// TODO(dbentley): also send a clear message for this runId
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

type dequeueAction struct {
	jobId   string
	claimed bool
}

func (a *dequeueAction) apply(st *schedulerState) []rpc {
	t := st.getJob(a.jobId)
	if a.claimed {
		t.status = jobRunning
	} else {
		t.status = jobFailed
	}
	return []rpc{respondWorkItem{a.jobId, a.claimed}}
}

func p(s string, args ...interface{}) {
	panic(fmt.Errorf(s, args...))
}
