package server

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"

	"github.com/wisechengyi/scoot/saga"
	"github.com/wisechengyi/scoot/scheduler/domain"
)

type taskStateByTaskID map[string]*taskState

// Contains all the information for a job in progress
// Note: Only Job, Saga, and Tasks are provided during scheduler recovery. Anything else must be initialized separately.
type jobState struct {
	Job            *domain.Job
	Saga           *saga.Saga   //saga associated with this job
	Tasks          []*taskState //ordered list of taskState
	EndingSaga     bool         //denotes whether an EndSagaMsg is in progress or not
	TasksCompleted int          //number of tasks that've been marked completed so far.
	TasksRunning   int          //number of tasks that've been scheduled or started.
	stateMu        sync.RWMutex
	JobKilled      bool      //indicates the job was killed
	TimeCreated    time.Time //when was this job first created
	TimeMarker     time.Time //when was this job last marked (i.e. for reporting purposes)

	jobClass                       string
	tasksByJobClassAndStartTimeSec map[taskClassAndStartKey]taskStateByJobIDTaskID
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

// the following types are used to access task State objects by class, startTime, taskID
type taskClassAndStartKey struct {
	class string
	start time.Time
}
type jobIDTaskIDKey struct {
	jobID  string
	taskID string
}
type taskStateByJobIDTaskID map[jobIDTaskIDKey]*taskState

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
func newJobState(job *domain.Job, jobClass string, saga *saga.Saga, taskDurations *lru.Cache,
	tasksByJobClassAndStartTimeSec map[taskClassAndStartKey]taskStateByJobIDTaskID, durationKeyExtractor func(string) string) *jobState {
	j := &jobState{
		Job:                            job,
		Saga:                           saga,
		Tasks:                          make([]*taskState, 0),
		EndingSaga:                     false,
		TasksCompleted:                 0,
		TasksRunning:                   0,
		JobKilled:                      false,
		TimeCreated:                    time.Now(),
		TimeMarker:                     time.Now(),
		jobClass:                       jobClass,
		tasksByJobClassAndStartTimeSec: tasksByJobClassAndStartTimeSec,
	}

	for _, taskDef := range job.Def.Tasks {
		var duration time.Duration
		durationKey := durationKeyExtractor(taskDef.TaskID)
		if taskDurations != nil {
			if iface, ok := taskDurations.Get(durationKey); !ok {
				addOrUpdateTaskDuration(taskDurations, durationKey, duration)
			} else {
				if ad, ok := iface.(*averageDuration); ok {
					duration = ad.duration
				}
			}
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
	}

	// Assumes Forward Recovery only, tasks are either
	// done or not done.  Scheduler currently doesn't support
	// scheduling compensating tasks.  In Progress tasks
	// are considered not done and will be rescheduled.
	sagaState := saga.GetState()
	for _, taskId := range sagaState.GetTaskIds() {
		if sagaState.IsTaskCompleted(taskId) {
			j.getTask(taskId).Status = domain.Completed
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
		if state.Status == domain.NotStarted {
			tasksToRun = append(tasksToRun, state)
		}
	}

	return tasksToRun
}

// Update JobState to reflect that a Task has been started
func (j *jobState) taskStarted(taskId string, tr *taskRunner) {
	taskState := j.getTask(taskId)
	start := time.Now()
	taskState.Status = domain.InProgress
	taskState.TimeStarted = start
	taskState.TaskRunner = tr
	taskState.NumTimesTried++
	j.stateMu.Lock()
	j.TasksRunning++
	j.stateMu.Unlock()

	// add the task to the map of tasks by start time
	startTimeSec := taskState.TimeStarted.Truncate(time.Second)
	j.addTaskToStartTimeMap(j.jobClass, taskState, startTimeSec)

	j.logInconsistentStateValues()
}

// Update JobState to reflect that a Task has been completed
// Running param: true if taskStarted was called for this taskId.
func (j *jobState) taskCompleted(taskId string, running bool) {
	taskState := j.getTask(taskId)
	startTimeSec := taskState.TimeStarted.Truncate(time.Second)
	taskState.Status = domain.Completed
	taskState.TimeStarted = nilTime
	taskState.TaskRunner = nil
	j.stateMu.Lock()
	j.TasksCompleted++
	if running {
		j.TasksRunning--
	}
	j.stateMu.Unlock()

	// remove the task from the map of tasks by start time
	j.removeTaskFromStartTimeMap(taskState.JobId, taskId, startTimeSec)

	j.logInconsistentStateValues()
}

// Update JobState to reflect that an error has occurred running this Task
func (j *jobState) errorRunningTask(taskId string, err error, preempted bool) {
	taskState := j.getTask(taskId)
	startTimeSec := taskState.TimeStarted.Truncate(time.Second)
	taskState.Status = domain.NotStarted
	taskState.TimeStarted = nilTime
	taskState.TaskRunner = nil
	j.stateMu.Lock()
	j.TasksRunning--
	j.stateMu.Unlock()
	if preempted {
		taskState.NumTimesTried--
	}

	j.removeTaskFromStartTimeMap(taskState.JobId, taskId, startTimeSec)

	j.logInconsistentStateValues()
}

func (j *jobState) GetNumRunning() int {
	j.stateMu.RLock()
	defer j.stateMu.RUnlock()
	return j.TasksRunning
}

func (j *jobState) GetNumCompleted() int {
	j.stateMu.RLock()
	defer j.stateMu.RUnlock()
	return j.TasksCompleted
}

// Returns the Current Job Status
func (j *jobState) getJobStatus() domain.Status {
	if j.GetNumCompleted() == len(j.Tasks) {
		return domain.Completed
	}
	return domain.InProgress
}

// addTaskToStartTimeMap add the running task to the map that bins running tasks by their class and start time
func (j *jobState) addTaskToStartTimeMap(jobClass string, task *taskState, startTimeSec time.Time) {
	if j.tasksByJobClassAndStartTimeSec == nil {
		log.Errorf("tasksByJobClassAndStartTime map not found.  Skipping adding task to it. jobID: %s, taskID:%s, jobClass:%s, startTime:%s", task.JobId, task.TaskId, jobClass, startTimeSec.Format("2006-01-02 15:04:05 -0700 MST"))
		return
	}
	classNStartBucketKey := taskClassAndStartKey{class: jobClass, start: startTimeSec}
	if _, ok := j.tasksByJobClassAndStartTimeSec[classNStartBucketKey]; !ok {
		j.tasksByJobClassAndStartTimeSec[classNStartBucketKey] = taskStateByJobIDTaskID{}
	}
	taskKey := jobIDTaskIDKey{jobID: task.JobId, taskID: task.TaskId}
	j.tasksByJobClassAndStartTimeSec[classNStartBucketKey][taskKey] = task
}

// removeTaskFromStartTimeMap remove the completed task from the map that bins running tasks by their class and start time
func (j *jobState) removeTaskFromStartTimeMap(jobID string, taskID string, startTimeSec time.Time) {
	if j.tasksByJobClassAndStartTimeSec == nil {
		log.Warnf("tasksByJobClassAndStartTime map not found.  Skipping removing task from it. jobID: %s, taskID: %s, jobClass:%s, startTime:%s", jobID, taskID, j.jobClass, startTimeSec.Format("2006-01-02 15:04:05 -0700 MST"))
		return
	}
	timeBucket := taskClassAndStartKey{class: j.jobClass, start: startTimeSec}
	if _, ok := j.tasksByJobClassAndStartTimeSec[timeBucket]; !ok {
		log.Warnf("no %s start time bucket found for the time %s. Skipping removing task %s_%s from it", j.jobClass, startTimeSec.Format("2006-01-02 15:04:05 -0700 MST"), jobID, taskID)
		return
	}
	taskKey := jobIDTaskIDKey{jobID: jobID, taskID: taskID}
	if _, ok := j.tasksByJobClassAndStartTimeSec[timeBucket][taskKey]; !ok {
		log.Warnf("task %s_%s was not found in %s time bucket found for the job %s.  Skipping removing task from it", jobID, taskID, j.jobClass, startTimeSec.Format("2006-01-02 15:04:05 -0700 MST"))
		return
	}
	delete(j.tasksByJobClassAndStartTimeSec[timeBucket], taskKey)
	if len(j.tasksByJobClassAndStartTimeSec[timeBucket]) == 0 {
		delete(j.tasksByJobClassAndStartTimeSec, timeBucket)
	}
}

func (j *jobState) logInconsistentStateValues() {
	// TODO remove before deploying to prod or if this slows staging down too much
	running := 0
	for classNStartKey, v := range j.tasksByJobClassAndStartTimeSec {
		if classNStartKey.class != j.jobClass {
			continue
		}
		for jobIDnTaskID, taskState := range v {
			if jobIDnTaskID.jobID != j.Job.Id {
				continue
			}
			if taskState.Status == domain.InProgress {
				running++
			}
		}
	}
	if running != j.TasksRunning {
		log.Errorf("inconsistent job state: tasksByJobClassAndStartTimeSec has %d running tasks for job %s,%s,%s, but jobState and %d running tasks",
			running, j.jobClass, j.Job.Def.Requestor, j.Job.Id, j.TasksRunning)
	}
}
