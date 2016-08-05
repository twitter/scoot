package scheduler

import (
	"fmt"
	"reflect"
	"runtime/debug"
	"testing"
	"time"

	"github.com/luci/go-render/render"

	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/sched"
)

func TestPlanner(t *testing.T) {
	assertPlan(t, state())

	assertPlan(t,
		state(workers(w("node1", "added", unix(0)))),
		pingWorker("node1"))
	assertPlan(t,
		state(workers(w("node1", "added"))))
	assertPlan(t, state(workers(w("node1", "avail"))))

	assertPlan(t, state(incoming(i("job1"))))
	assertPlan(t, state(
		workers(w("node1", "added", unix(0)), w("node2", "added"), w("node3", "busy"), w("node4", "down")),
		incoming(simpleJob(("job1")))),
		pingWorker("node1"))
	assertPlan(t, state(
		workers(w("node1", "avail")),
		incoming(simpleJob("job1"))),
		startJob("job1"),
		startRun("job1", "task1", "node1"))

	assertPlan(t, state(
		workers(w("node1", "avail")),
		jobs(j("job1", task("task1", "wait")))),
		startRun("job1", "task1", "node1"))

	assertPlan(t, state(jobs(j("job1", jobNew))))
	assertPlan(t, state(jobs(j("job1", jobPersisted))),
		dequeue("job1"))
	assertPlan(t, state(jobs(j("job1", jobCannotPersist))),
		dequeue("job1", false))
	assertPlan(t, state(jobs(j("job1", jobRunning))),
		endJob("job1"))

	// Don't run task1 if we're already running
	assertPlan(t, state(
		workers(w("node1", "busy"),
			w("node2", "avail")),
		jobs(j("job1",
			taskRun("task1", "node1", "r1")))))

	// Ping the worker if it's been too long
	assertPlan(t, state(
		workers(w("node1", "busy", unix(7)),
			w("node2", "avail")),
		jobs(j("job1",
			taskRun("task1", "node1", "r1")))),
		pingWorker("node1"))

	// Finish when our run is complete
	assertPlan(t, state(
		workers(w("node1", "avail", complete("r1"))),
		jobs(j("job1",
			taskRun("task1", "node1", "r1")))),
		endTask("job1", "task1"))

	// Don't finish just because the worker is available
	assertPlan(t, state(
		workers(w("node1", "avail")),
		jobs(j("job1", taskRun("task1", "node1", "r1")))))

	// Make sure we finish the right task
	assertPlan(t, state(
		workers(
			w("node1", "avail"),
			w("node2", "avail", complete("r1"))),
		jobs(j("job1",
			taskRun("task1", "node1", "r1"),
			taskRun("task2", "node2", "r1")))),
		endTask("job1", "task2"))

	assertPlan(t, state(
		workers(w("node1", "avail", complete("r1"))),
		jobs(j("job1", jobRunning, taskRun("task1", "node1", "r1")))),
		endTask("job1", "task1"),
		endJob("job1"))

	// Nothing to do if we're done
	assertPlan(t, state(
		jobs(j("job1", jobDone, task("task1", "done")))))
}

func state(data ...interface{}) *schedulerState {
	st := &schedulerState{}
	st.now = now
	for _, d := range data {
		switch d := d.(type) {
		case []*workerState:
			if st.workers != nil {
				panic(fmt.Errorf("workers already set"))
			}
			st.workers = d
		case []*jobState:
			if st.jobs != nil {
				panic(fmt.Errorf("jobs already set"))
			}
			st.jobs = d
		case []sched.Job:
			if st.incoming != nil {
				panic(fmt.Errorf("incoming already set"))
			}
			st.incoming = d
		default:
			panic(fmt.Errorf("bad type for arg to func state %T %v", d, d))
		}
	}
	return st
}

func workers(workers ...*workerState) []*workerState {
	return workers
}

func w(id string, status string, data ...interface{}) *workerState {
	r := &workerState{
		id:     id,
		status: workerStatusFromName(status),
		// By default, make these up-to-date
		lastSend: now,
		lastRecv: now,
	}

	setLastSend := false

	for _, d := range data {
		switch d := d.(type) {
		case time.Time:
			if setLastSend {
				r.lastRecv = d
			} else {
				r.lastSend = d
				setLastSend = true
			}
		case runner.ProcessStatus:
			r.runs = append(r.runs, d)
		default:
			panic(fmt.Errorf("bad type for arg to func w %T %v", d, d))
		}

	}

	if r.lastRecv.After(r.lastSend) {
		r.lastRecv = r.lastSend
	}

	return r
}

func unix(secs int64) time.Time {
	return time.Unix(secs, 0)
}

var now = unix(10000)

func complete(runId string) (r runner.ProcessStatus) {
	r.RunId = runner.RunId(runId)
	r.State = runner.COMPLETE
	return r
}

func jobs(jobs ...*jobState) []*jobState {
	return jobs
}

func j(id string, data ...interface{}) *jobState {
	r := &jobState{
		id: id,
	}

	for _, d := range data {
		switch d := d.(type) {
		case *taskState:
			r.tasks = append(r.tasks, d)
		case jobStatus:
			r.status = d
		default:
			panic(fmt.Errorf("bad type for arg to func j %T %v", d, d))
		}
	}
	return r
}

func task(id string, status string) *taskState {
	return &taskState{
		id:        id,
		status:    taskStatusFromName(status),
		runningOn: "",
	}
}

func taskRun(id string, workerId string, runId string) *taskState {
	return &taskState{
		id:        id,
		status:    taskRunning,
		runningOn: workerId,
		runningAs: runner.RunId(runId),
	}
}

func incoming(is ...sched.Job) []sched.Job {
	return is
}

func simpleJob(id string) sched.Job {
	tasks := make(map[string]sched.TaskDefinition)
	for i := 0; i < 10; i++ {
		tasks[fmt.Sprintf("task1")] = sched.TaskDefinition{
			Command: runner.Command{
				Argv:       []string{"echo", "hello world"},
				SnapshotId: "",
			},
		}
	}
	return sched.Job{
		Id: id,
		Def: sched.JobDefinition{
			Tasks: tasks,
		},
	}
}

func i(id string, t ...taskDef) sched.Job {
	return sched.Job{
		Id:  id,
		Def: sched.JobDefinition{},
	}
}

type taskDef struct {
	id  string
	def sched.TaskDefinition
}

func workerStatusFromName(name string) workerStatus {
	switch name {
	case "added":
		return workerAdded
	case "avail":
		return workerAvailable
	case "busy":
		return workerBusy
	case "down":
		return workerDown
	}
	panic(fmt.Errorf("Unknown worker status: %v", name))
}

func taskStatusFromName(name string) taskStatus {
	switch name {
	case "wait":
		return taskWaiting
	case "run":
		return taskRunning
	case "done":
		return taskDone
	}
	panic(fmt.Errorf("Unknown task status: %v", name))
}

func assertPlan(t *testing.T, st *schedulerState, expected ...action) {
	p := makePlanner(st)
	p.plan()
	actual := p.actions
	if len(actual) != len(expected) {
		t.Errorf(
			"Plan error: different lengths: %d %d\n"+
				"Expected: %v\n"+
				"Actual:   %v\n"+
				"State: %v\n"+
				"Stack: %s\n",
			len(expected), len(actual),
			render.Render(expected), render.Render(actual),
			render.Render(st), debug.Stack())
		return
	}

	for i, _ := range actual {
		if !reflect.DeepEqual(actual[i], expected[i]) {
			t.Errorf(
				"Plan error: different order at position %d of %d.\n"+
					"Expected %v\n"+
					"Actual   %v\n"+
					"All Expected  %v\n"+
					"All Actual    %v\n"+
					"State: %v\n"+
					"Stack: %s\n",
				i, len(actual),
				render.Render(expected[i]), render.Render(actual[i]),
				render.Render(expected), render.Render(actual),
				render.Render(st), debug.Stack())
		}
	}

	// TODO(dbentley): now apply, then plan again, and make sure the plan is empty
}

func pingWorker(id string) *pingWorkerAction {
	return &pingWorkerAction{id: id}
}

func startJob(id string) *startJobAction {
	return &startJobAction{id: id}
}

func startRun(jobId string, taskId string, workerId string) *startRunAction {
	return &startRunAction{jobId: jobId, taskId: taskId, workerId: workerId}
}

func endTask(jobId string, taskId string) *endTaskAction {
	return &endTaskAction{jobId: jobId, taskId: taskId}
}

func endJob(jobId string) *endJobAction {
	return &endJobAction{jobId: jobId}
}

func dequeue(jobId string, cs ...bool) *dequeueAction {
	claimed := true
	if len(cs) > 0 {
		claimed = cs[0]
	}
	return &dequeueAction{jobId: jobId, claimed: claimed}
}
