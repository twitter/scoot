package scheduler

import (
	"fmt"
	"github.com/scootdev/scoot/sched"
)

type pingWorkerAction struct {
	id string
}

func pingWorker(id string) *pingWorkerAction {
	return &pingWorkerAction{id: id}
}

func (a *pingWorkerAction) apply(st *schedulerState) {
	st.getWorker(a.id).status = workerPinged
}

type startJobAction struct {
	id string
}

func startJob(id string) *startJobAction {
	return &startJobAction{id: id}
}

func (a *startJobAction) apply(st *schedulerState) {
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
}

type startRunAction struct {
	jobId    string
	taskId   string
	workerId string
}

func startRun(jobId string, taskId string, workerId string) *startRunAction {
	return &startRunAction{jobId: jobId, taskId: taskId, workerId: workerId}
}

func (a *startRunAction) apply(st *schedulerState) {
	t := st.getTask(a.jobId, a.taskId)
	t.status = taskRunning
	t.runningOn = a.workerId
	w := st.getWorker(a.workerId)
	w.status = workerBusy
}

type endTaskAction struct {
	jobId  string
	taskId string
}

func endTask(jobId string, taskId string) *endTaskAction {
	return &endTaskAction{jobId: jobId, taskId: taskId}
}

func (a *endTaskAction) apply(st *schedulerState) {
	t := st.getTask(a.jobId, a.taskId)
	t.status = taskDone
	t.runningOn = ""
}

type endJobAction struct {
	jobId string
}

func endJob(jobId string) *endJobAction {
	return &endJobAction{jobId: jobId}
}

func (a *endJobAction) apply(st *schedulerState) {
	t := st.getJob(a.jobId)
	t.status = jobDone
}

func p(s string, args ...interface{}) {
	panic(fmt.Errorf(s, args...))
}
