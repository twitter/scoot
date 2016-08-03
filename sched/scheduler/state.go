package scheduler

import (
	"log"
	"sort"

	"github.com/scootdev/scoot/sched"
	"github.com/scootdev/scoot/workerapi"
)

type workerStatus int

const (
	workerAdded workerStatus = iota
	workerPinged
	workerAvailable
	workerBusy
	workerDown
)

type workerState struct {
	id     string
	status workerStatus
}

type taskStatus int

const (
	taskWaiting taskStatus = iota
	taskRunning
	taskDone
)

type taskState struct {
	id     string
	status taskStatus
	def    sched.TaskDefinition
	// The ID for the worker this task is currently running on
	// Only valid for taskRunning
	runningOn string
}

type jobStatus int

const (
	jobNew jobStatus = iota
	jobPersisted
	jobRunning
	jobDone
)

type jobState struct {
	id     string
	status jobStatus
	tasks  []*taskState
}

type schedulerState struct {
	workers  []*workerState
	jobs     []*jobState
	incoming []sched.Job
	err      error
}

func (s *schedulerState) getJob(jobId string) *jobState {
	for _, j := range s.jobs {
		if j.id == jobId {
			return j
		}
	}
	return nil
}

func (s *schedulerState) getTask(jobId string, taskId string) *taskState {
	j := s.getJob(jobId)
	for _, t := range j.tasks {
		if t.id == taskId {
			return t
		}
	}
	return nil
}

func (s *schedulerState) getWorker(id string) *workerState {
	for _, w := range s.workers {
		if w.id == id {
			return w
		}
	}
	return nil
}

func (s *schedulerState) availableWorkers() (r []*workerState) {
	// XXX(dbentley): here to prevent compile errors
	if false {
		log.Println()
	}
	for _, w := range s.workers {
		if w.status == workerAvailable {
			r = append(r, w)
		}
	}
	return r
}

func initialJobState(job sched.Job) *jobState {
	taskIds := make([]string, len(job.Def.Tasks))
	i := 0
	for taskId, _ := range job.Def.Tasks {
		taskIds[i] = taskId
		i++
	}
	sort.Strings(taskIds)

	tasks := make([]*taskState, len(taskIds))

	for i, taskId := range taskIds {
		tasks[i] = &taskState{
			id:        taskId,
			status:    taskWaiting,
			def:       job.Def.Tasks[taskId],
			runningOn: "",
		}
	}

	return &jobState{
		id:    job.Id,
		tasks: tasks,
	}
}

func (s *schedulerState) updateWorker(a *pingWorkerAction, status *workerapi.WorkerStatus, err error) {
	ws := s.getWorker(a.id)
	if ws == nil {
		return
	}
	if err != nil {
		ws.status = workerDown
		return
	}
	for _, rs := range status.Runs {
		if rs.State.IsBusy() {
			ws.status = workerBusy
		}
	}
	ws.status = workerAvailable
}

func (s *schedulerState) markRunComplete(a *startRunAction, err error) {
	w := s.getWorker(a.workerId)
	if w != nil {
		w.status = workerAvailable
	}
}
