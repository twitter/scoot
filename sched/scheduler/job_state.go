package scheduler

import (
	"time"

	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/sched"
)

// Contains all the information for a job in progress
// Note: Only Job, Saga, and Tasks are provided during scheduler recovery. Anything else must be initialized separately.
type jobState struct {
	Job            *sched.Job
	Saga           *saga.Saga   //saga associated with this job
	Tasks          []*taskState //ordered list of taskState
	EndingSaga     bool         //denotes whether an EndSagaMsg is in progress or not
	TasksCompleted int          //number of tasks that've been marked completed so far.
	TasksRunning   int          //number of tasks that've been scheduled or started.
	JobKilled      bool         //indicates the job was killed
}

// Contains all the information for a specified task
type taskState struct {
	JobId         string
	TaskId        string
	Def           sched.TaskDefinition
	Status        sched.Status
	TimeStarted   time.Time
	NumTimesTried int
	TaskRunner    *taskRunner
}

// Creates a New Job State based on the specified Job and Saga
// The jobState will reflect any previous progress made on this
// job and logged to the Sagalog
func newJobState(job *sched.Job, saga *saga.Saga) *jobState {
	j := &jobState{
		Job:            job,
		Saga:           saga,
		Tasks:          make([]*taskState, 0),
		EndingSaga:     false,
		TasksCompleted: 0,
		TasksRunning:   0,
		JobKilled:      false,
	}

	for _, taskDef := range job.Def.Tasks {
		task := &taskState{
			JobId:         job.Id,
			TaskId:        taskDef.TaskID,
			Def:           taskDef,
			Status:        sched.NotStarted,
			TimeStarted:   nilTime,
			NumTimesTried: 0,
		}
		j.Tasks = append(j.Tasks, task)
	}

	// Assumes Forward Recovery only, tasks are either
	// done or not done.  Scheduler currently doesn't support
	// scheduling compensating tasks.  In Progress tasks
	// are considered not done and will be rescheduled.
	for _, taskId := range saga.GetState().GetTaskIds() {
		if saga.GetState().IsTaskCompleted(taskId) {
			j.getTask(taskId).Status = sched.Completed
			j.TasksCompleted++
		}
	}

	return j
}

// Helper, assumes that taskId is present given a consistent jobState.
func (j *jobState) getTask(taskId string) *taskState {
	for _, task := range j.Tasks {
		if task.TaskId == taskId {
			return task
		}
	}
	return nil
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
func (j *jobState) taskStarted(taskId string, tr *taskRunner) {
	taskState := j.getTask(taskId)
	taskState.Status = sched.InProgress
	taskState.TimeStarted = time.Now()
	taskState.TaskRunner = tr
	taskState.NumTimesTried++
	j.TasksRunning++
}

// Update JobState to reflect that a Task has been completed
func (j *jobState) taskCompleted(taskId string, running bool) {
	taskState := j.getTask(taskId)
	taskState.Status = sched.Completed
	taskState.TimeStarted = nilTime
	taskState.TaskRunner = nil
	j.TasksCompleted++
	if running {
		j.TasksRunning--
	}
}

// Update JobState to reflect that an error has occurred running this Task
func (j *jobState) errorRunningTask(taskId string, err error, preempted bool) {
	taskState := j.getTask(taskId)
	taskState.Status = sched.NotStarted
	taskState.TimeStarted = nilTime
	j.TasksRunning--
	if preempted {
		taskState.NumTimesTried--
	}
}

// Returns the Current Job Status
func (j *jobState) getJobStatus() sched.Status {
	if j.TasksCompleted == len(j.Tasks) {
		return sched.Completed
	}
	return sched.InProgress
}
