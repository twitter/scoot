package server

import (
	"math"
	"time"

	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/scheduler/domain"
)

// Contains all the information for a job in progress
// Note: Only Job, Saga, and Tasks are provided during scheduler recovery. Anything else must be initialized separately.
type jobState struct {
	Job            *domain.Job
	Saga           *saga.Saga   //saga associated with this job
	Tasks          []*taskState //ordered list of taskState
	EndingSaga     bool         //denotes whether an EndSagaMsg is in progress or not
	TasksCompleted int          //number of tasks that've been marked completed so far.
	TasksRunning   int          //number of tasks that've been scheduled or started.
	JobKilled      bool         //indicates the job was killed
	TimeCreated    time.Time    //when was this job first created
	TimeMarker     time.Time    //when was this job last marked (i.e. for reporting purposes)

	// track tasks by state (completed, running, not started) for priority based scheduling algorithm
	// TODO remove if we don't use the priority based scheduling algorithm
	Completed  map[string]*taskState
	Running    map[string]*taskState
	NotStarted map[string]*taskState
}

// Contains all the information for a specified task
type taskState struct {
	JobId         string
	TaskId        string
	Def           domain.TaskDefinition
	Status        domain.Status
	TimeStarted   time.Time
	NumTimesTried int
	TaskRunner    *taskRunner
	AvgDuration   time.Duration //average duration for previous runs with this taskId, if any.
}

type taskStatesByDuration []*taskState

func (s taskStatesByDuration) Len() int {
	return len(s)
}
func (s taskStatesByDuration) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s taskStatesByDuration) Less(i, j int) bool {
	return s[i].AvgDuration < s[j].AvgDuration
}

// Creates a New Job State based on the specified Job and Saga
// The jobState will reflect any previous progress made on this job and logged to the Sagalog
// Note: taskDurations is optional and only used to enable sorts using taskStatesByDuration above.
func newJobState(job *domain.Job, saga *saga.Saga, taskDurations map[string]*averageDuration) *jobState {
	j := &jobState{
		Job:            job,
		Saga:           saga,
		Tasks:          make([]*taskState, 0),
		EndingSaga:     false,
		TasksCompleted: 0,
		TasksRunning:   0,
		JobKilled:      false,
		TimeCreated:    time.Now(),
		TimeMarker:     time.Now(),
		Completed:      make(map[string]*taskState),
		Running:        make(map[string]*taskState),
		NotStarted:     make(map[string]*taskState),
	}

	for _, taskDef := range job.Def.Tasks {
		var duration time.Duration
		if taskDurations != nil {
			if avgDur, ok := taskDurations[taskDef.TaskID]; !ok || avgDur.duration == 0 {
				taskDurations[taskDef.TaskID] = &averageDuration{}
				taskDurations[taskDef.TaskID].update(math.MaxInt64) // Set max duration if we don't have the average duration.
			}
			duration = taskDurations[taskDef.TaskID].duration
		}
		task := &taskState{
			JobId:         job.Id,
			TaskId:        taskDef.TaskID,
			Def:           taskDef,
			Status:        domain.NotStarted,
			TimeStarted:   nilTime,
			NumTimesTried: 0,
			AvgDuration:   duration,
		}
		j.Tasks = append(j.Tasks, task)
		j.NotStarted[task.TaskId] = task
	}

	// Assumes Forward Recovery only, tasks are either
	// done or not done.  Scheduler currently doesn't support
	// scheduling compensating tasks.  In Progress tasks
	// are considered not done and will be rescheduled.
	for _, taskId := range saga.GetState().GetTaskIds() {
		if saga.GetState().IsTaskCompleted(taskId) {
			j.getTask(taskId).Status = domain.Completed
			j.TasksCompleted++
			if _, ok := j.Running[taskId]; ok {
				delete(j.Running, taskId)
			} else if _, ok = j.NotStarted[taskId]; ok { // TODO what should we really do?
				delete(j.NotStarted, taskId)
			}
			j.Completed[taskId] = j.getTask(taskId)

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
		if state.Status == domain.NotStarted {
			tasksToRun = append(tasksToRun, state)
		}
	}

	return tasksToRun
}

// Update JobState to reflect that a Task has been started
func (j *jobState) taskStarted(taskId string, tr *taskRunner) {
	taskState := j.getTask(taskId)
	taskState.Status = domain.InProgress
	taskState.TimeStarted = time.Now()
	taskState.TaskRunner = tr
	taskState.NumTimesTried++
	j.TasksRunning++
	if _, ok := j.NotStarted[taskId]; ok {
		delete(j.NotStarted, taskId)
	} else if _, ok = j.Completed[taskId]; ok { // TODO what should we really do?
		delete(j.Completed, taskId)
	}
	j.Running[taskId] = taskState
}

// Update JobState to reflect that a Task has been completed
// Running param: true if taskStarted was called for this taskId.
func (j *jobState) taskCompleted(taskId string, running bool) {
	taskState := j.getTask(taskId)
	taskState.Status = domain.Completed
	taskState.TimeStarted = nilTime
	taskState.TaskRunner = nil
	j.TasksCompleted++
	if running {
		j.TasksRunning--
	}
	if _, ok := j.Running[taskId]; ok {
		delete(j.Running, taskId)
	} else if _, ok = j.NotStarted[taskId]; ok { // TODO what should we really do?
		delete(j.NotStarted, taskId)
	}
	j.Completed[taskId] = taskState
}

// Update JobState to reflect that an error has occurred running this Task
func (j *jobState) errorRunningTask(taskId string, err error, preempted bool) {
	taskState := j.getTask(taskId)
	taskState.Status = domain.NotStarted
	taskState.TimeStarted = nilTime
	taskState.TaskRunner = nil
	j.TasksRunning--
	if preempted {
		taskState.NumTimesTried--
	}
	if _, ok := j.Running[taskId]; ok {
		delete(j.Running, taskId)
	} else if _, ok = j.Completed[taskId]; ok { // TODO what should we really do?
		delete(j.Completed, taskId)
	}
	j.NotStarted[taskId] = taskState
}

// Returns the Current Job Status
func (j *jobState) getJobStatus() domain.Status {
	if j.TasksCompleted == len(j.Tasks) {
		return domain.Completed
	}
	return domain.InProgress
}
