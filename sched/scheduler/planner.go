package scheduler

type planner struct {
	st      *schedulerState
	actions []action
}

func makePlanner(st *schedulerState) *planner {
	return &planner{
		st:      st,
		actions: nil,
	}
}

func (p *planner) plan() {
	// TODO(dbentley): log c.current
	p.pingNewWorkers()
	p.finishTasks()
	p.assignWorkers()
	p.endJobs()
}

func (p *planner) pingNewWorkers() {
	for _, w := range p.st.workers {
		if w.status == workerAdded {
			p.action(pingWorker(w.id))
		}
	}
}

func (p *planner) finishTasks() {
	for _, j := range p.st.jobs {
		for _, t := range j.tasks {
			if t.status == taskRunning {
				w := p.st.getWorker(t.runningOn)
				if w.status == workerAvailable {
					p.action(endTask(j.id, t.id))
				}
			}
		}
	}
}

func (p *planner) assignWorkers() {
	avail := p.st.availableWorkers()
	for _, w := range avail {
		for _, j := range p.st.jobs {
			if p.offerWorkerToJob(w, j) {
				continue
			}
		}
		for _, _ = range p.st.incoming {
			// Start the incoming job
			p.action(startJob(p.st.incoming[0].Id))
			j := p.st.jobs[len(p.st.jobs)-1]
			if p.offerWorkerToJob(w, j) {
				continue
			}
		}
	}
}

const MAX_WORKERS_PER_JOB = 5

// Offer this worker to the job. Returns whether the worker was
// assigned.
func (p *planner) offerWorkerToJob(w *workerState, j *jobState) bool {
	numRunning := 0
	for _, t := range j.tasks {
		if t.status == taskRunning {
			numRunning++
		}
	}
	if numRunning > MAX_WORKERS_PER_JOB {
		// Don't hog the cluster, return
		return false
	}
	for _, t := range j.tasks {
		if t.status == taskWaiting {
			p.action(startRun(j.id, t.id, w.id))
			return true
		}
	}
	return false
}

func (p *planner) endJobs() {
	for _, j := range p.st.jobs {
		if j.status == jobDone {
			continue
		}
		done := true
		for _, t := range j.tasks {
			if t.status != taskDone {
				done = false
				break
			}
		}
		if done {
			p.action(endJob(j.id))
		}
	}
}

func (p *planner) action(a action) {
	p.actions = append(p.actions, a)
	a.apply(p.st)
}
