package scheduler

import (
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched"
)

// Contains all the information for a job in progress
type jobState struct {
	Job        sched.Job
	Saga       *saga.Saga            // saga associated with this job
	Tasks      map[string]*taskState //taskId to taskState
	EndingSaga bool                  //denotes whether an EndSagaMsg is in progress or not
}

// Contains all the information for a specified task
type taskState struct {
	JobId         string
	TaskId        string
	Def           sched.TaskDefinition
	Status        sched.Status
	NumTimesTried int
}

func newJobState(job sched.Job, saga *saga.Saga) *jobState {
	j := &jobState{
		Job:        job,
		Saga:       saga,
		Tasks:      make(map[string]*taskState),
		EndingSaga: false,
	}

	for taskId, taskDef := range job.Def.Tasks {
		j.Tasks[taskId] = &taskState{
			JobId:         job.Id,
			TaskId:        taskId,
			Def:           taskDef,
			Status:        sched.NotStarted,
			NumTimesTried: 0,
		}
	}

	return j
}

// Returns a list of taskIds that can be scheduled currently.
func (j *jobState) getUnScheduledTasks() []*taskState {

	var tasksToRun []*taskState

	for _, state := range j.Tasks {
		if state.Status == sched.NotStarted {
			tasksToRun = append(tasksToRun, state)
		}
	}

	return tasksToRun
}

// Update JobState to reflect that a Task has been started
func (j *jobState) taskStarted(taskId string) {
	taskState := j.Tasks[taskId]
	taskState.Status = sched.InProgress
	taskState.NumTimesTried++
}

// Update JobState to reflect that a Task has been completed
func (j *jobState) taskCompleted(taskId string) {
	taskState := j.Tasks[taskId]
	taskState.Status = sched.Completed
}

// Update JobState to reflect that an error has occurred running this Task
func (j *jobState) errorRunningTask(taskId string, err error) {
	taskState := j.Tasks[taskId]
	taskState.Status = sched.NotStarted
}

// Returns the Current Job Status
func (j *jobState) getJobStatus() sched.Status {
	for _, tState := range j.Tasks {
		if tState.Status != sched.Completed {
			return sched.InProgress
		}
	}
	return sched.Completed
}
