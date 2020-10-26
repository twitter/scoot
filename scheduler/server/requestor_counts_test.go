package server

import (
	"strconv"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/twitter/scoot/saga/sagalogs"
	"github.com/twitter/scoot/scheduler/domain"
)

func Test_StatefulSchedulerRequestorCounts(t *testing.T) {
	origLevel := log.GetLevel()
	log.SetLevel(log.ErrorLevel)
	sc := sagalogs.MakeInMemorySagaCoordinatorNoGC()
	s, _, statsRegistry := initializeServices(sc, false)
	s.SetSchedulingAlg(NewOrigSchedulingAlg())

	// create a series of p0 through p2 tasks and run one scheduling iteration
	phase1Profiles := []map[string]string{
		{"numTasks": "5", "requestor": "p0Requestor", "priority": "0"},
		{"numTasks": "3", "requestor": "p1Requestor", "priority": "1"},
		{"numTasks": "1", "requestor": "p2Requestor", "priority": "2"},
	}
	jobIds := addRequestorTestJobsToScheduler(phase1Profiles, s)
	s.step()                          // start the first set of tasks
	time.Sleep(50 * time.Millisecond) // let task threads run to complete
	s.step()                          // get first set of completed tasks and start next set
	// verify that all tasks in job1 and job 3 ran and 1 task from job2 ran and 2 are waiting
	ok := verifyJobStatus("phase1: verify job0 still running", jobIds[0], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.InProgress, domain.InProgress, domain.InProgress}, s, t)
	ok = ok && verifyJobStatus("phase1: verify job1 still running", jobIds[1], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.InProgress}, s, t)
	ok = ok && verifyJobStatus("phase1: verify job2 still running", jobIds[2], domain.Completed,
		[]domain.Status{domain.Completed}, s, t)

	// check the gauges
	ok = ok && checkGauges("p0Requestor", map[string]int{"jobRunning": 1, "jobsWaitingToStart": 0,
		"numRunningTasks": 3, "numWaitingTasks": 0}, s, t, statsRegistry)
	ok = ok && checkGauges("p1Requestor", map[string]int{"jobRunning": 1, "jobsWaitingToStart": 0,
		"numRunningTasks": 1, "numWaitingTasks": 0}, s, t, statsRegistry)
	ok = ok && checkGauges("p2Requestor", map[string]int{"jobRunning": 0, "jobsWaitingToStart": 0,
		"numRunningTasks": 0, "numWaitingTasks": 0}, s, t, statsRegistry)
	if !ok {
		t.Fatal("failed first requestor gauges test")
	}

	phase2Profiles := []map[string]string{
		{"numTasks": "6", "requestor": "p0Requestor", "priority": "0"},
		{"numTasks": "4", "requestor": "p1Requestor", "priority": "1"},
		{"numTasks": "2", "requestor": "p2Requestor", "priority": "2"},
		{"numTasks": "6", "requestor": "p0Requestor", "priority": "0"},
		{"numTasks": "2", "requestor": "p0Requestor", "priority": "0"},
		{"numTasks": "1", "requestor": "p0Requestor", "priority": "0"},
		{"numTasks": "1", "requestor": "p0Requestor", "priority": "0"},
	}

	j := addRequestorTestJobsToScheduler(phase2Profiles, s)
	jobIds = append(jobIds, j[:]...)
	time.Sleep(50 * time.Millisecond) // let prior phase's tasks complete
	s.step()                          // start the next set of tasks
	ok = verifyJobStatus("phase2: verify job3 still running", jobIds[3], domain.InProgress,
		[]domain.Status{domain.InProgress, domain.NotStarted, domain.NotStarted, domain.NotStarted, domain.NotStarted,
			domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase2: verify job4 still running", jobIds[4], domain.InProgress,
		[]domain.Status{domain.InProgress, domain.NotStarted, domain.NotStarted, domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase2: verify job5 still running", jobIds[5], domain.InProgress,
		[]domain.Status{domain.InProgress, domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase2: verify job7 waiting to start", jobIds[6], domain.InProgress,
		[]domain.Status{domain.InProgress, domain.NotStarted, domain.NotStarted, domain.NotStarted, domain.NotStarted,
			domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase2: verify job7 waiting to start", jobIds[7], domain.InProgress,
		[]domain.Status{domain.InProgress, domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase2: verify job8 waiting to start", jobIds[8], domain.InProgress,
		[]domain.Status{domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase2: verify job9 waiting to start", jobIds[9], domain.InProgress,
		[]domain.Status{domain.NotStarted}, s, t)

	// check the gauges
	ok = ok && checkGauges("p0Requestor", map[string]int{"jobRunning": 3, "jobsWaitingToStart": 2,
		"numRunningTasks": 3, "numWaitingTasks": 13}, s, t, statsRegistry)
	ok = ok && checkGauges("p1Requestor", map[string]int{"jobRunning": 1, "jobsWaitingToStart": 0,
		"numRunningTasks": 1, "numWaitingTasks": 3}, s, t, statsRegistry)
	ok = ok && checkGauges("p2Requestor", map[string]int{"jobRunning": 1, "jobsWaitingToStart": 0,
		"numRunningTasks": 1, "numWaitingTasks": 1}, s, t, statsRegistry)
	if !ok {
		t.Fatal("failed first requestor gauges test")
	}

	time.Sleep(50 * time.Millisecond) // let prior phase's tasks' threads complete
	s.step()                          // start the next set of tasks
	ok = verifyJobStatus("phase3: verify job3 still running", jobIds[3], domain.InProgress,
		[]domain.Status{domain.Completed, domain.InProgress, domain.NotStarted, domain.NotStarted, domain.NotStarted,
			domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase3: verify job4 still running", jobIds[4], domain.InProgress,
		[]domain.Status{domain.Completed, domain.InProgress, domain.NotStarted, domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase3: verify job5 still running", jobIds[5], domain.InProgress,
		[]domain.Status{domain.Completed, domain.InProgress}, s, t)
	ok = ok && verifyJobStatus("phase3: verify job7 waiting to start", jobIds[6], domain.InProgress,
		[]domain.Status{domain.Completed, domain.InProgress, domain.NotStarted, domain.NotStarted, domain.NotStarted,
			domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase3: verify job7 waiting to start", jobIds[7], domain.InProgress,
		[]domain.Status{domain.Completed, domain.InProgress}, s, t)
	ok = ok && verifyJobStatus("phase3: verify job8 waiting to start", jobIds[8], domain.InProgress,
		[]domain.Status{domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase3: verify job9 waiting to start", jobIds[9], domain.InProgress,
		[]domain.Status{domain.NotStarted}, s, t)

	// check the gauges
	ok = ok && checkGauges("p0Requestor", map[string]int{"jobRunning": 3, "jobsWaitingToStart": 2,
		"numRunningTasks": 3, "numWaitingTasks": 10}, s, t, statsRegistry)
	ok = ok && checkGauges("p1Requestor", map[string]int{"jobRunning": 1, "jobsWaitingToStart": 0,
		"numRunningTasks": 1, "numWaitingTasks": 2}, s, t, statsRegistry)
	ok = ok && checkGauges("p2Requestor", map[string]int{"jobRunning": 1, "jobsWaitingToStart": 0,
		"numRunningTasks": 1, "numWaitingTasks": 0}, s, t, statsRegistry)
	if !ok {
		t.Fatal("failed first requestor gauges test")
	}

	time.Sleep(50 * time.Millisecond) // let prior phase's tasks' threads complete
	s.step()                          // start the next set of tasks
	ok = verifyJobStatus("phase4: verify job3 still running", jobIds[3], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.InProgress, domain.NotStarted, domain.NotStarted,
			domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase4: verify job4 still running", jobIds[4], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.InProgress, domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase4: verify job5 still running", jobIds[5], domain.Completed,
		[]domain.Status{domain.Completed, domain.Completed}, s, t)
	ok = ok && verifyJobStatus("phase4: verify job6 waiting to start", jobIds[6], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.InProgress, domain.NotStarted, domain.NotStarted,
			domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase4: verify job7 waiting to start", jobIds[7], domain.Completed,
		[]domain.Status{domain.Completed, domain.Completed}, s, t)
	ok = ok && verifyJobStatus("phase4: verify job8 waiting to start", jobIds[8], domain.InProgress,
		[]domain.Status{domain.InProgress}, s, t)
	ok = ok && verifyJobStatus("phase4: verify job9 waiting to start", jobIds[9], domain.InProgress,
		[]domain.Status{domain.InProgress}, s, t)

	// check the gauges
	ok = ok && checkGauges("p0Requestor", map[string]int{"jobRunning": 4, "jobsWaitingToStart": 0,
		"numRunningTasks": 4, "numWaitingTasks": 6}, s, t, statsRegistry)
	ok = ok && checkGauges("p1Requestor", map[string]int{"jobRunning": 1, "jobsWaitingToStart": 0,
		"numRunningTasks": 1, "numWaitingTasks": 1}, s, t, statsRegistry)
	ok = ok && checkGauges("p2Requestor", map[string]int{"jobRunning": 0, "jobsWaitingToStart": 0,
		"numRunningTasks": 0, "numWaitingTasks": 0}, s, t, statsRegistry)
	if !ok {
		t.Fatal("failed first requestor gauges test")
	}

	time.Sleep(50 * time.Millisecond) // let prior phase's tasks' threads complete
	s.step()                          // start the next set of tasks
	ok = verifyJobStatus("phase5: verify job3 still running", jobIds[3], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.Completed, domain.InProgress, domain.InProgress,
			domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase5: verify job4 still running", jobIds[4], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.Completed, domain.InProgress}, s, t)
	ok = ok && verifyJobStatus("phase5: verify job6 waiting to start", jobIds[6], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.Completed, domain.InProgress, domain.InProgress,
			domain.NotStarted}, s, t)
	ok = ok && verifyJobStatus("phase5: verify job8 waiting to start", jobIds[8], domain.Completed,
		[]domain.Status{domain.Completed}, s, t)
	ok = ok && verifyJobStatus("phase5: verify job9 waiting to start", jobIds[9], domain.Completed,
		[]domain.Status{domain.Completed}, s, t)

	// check the gauges
	ok = ok && checkGauges("p0Requestor", map[string]int{"jobRunning": 2, "jobsWaitingToStart": 0,
		"numRunningTasks": 4, "numWaitingTasks": 2}, s, t, statsRegistry)
	ok = ok && checkGauges("p1Requestor", map[string]int{"jobRunning": 1, "jobsWaitingToStart": 0,
		"numRunningTasks": 1, "numWaitingTasks": 0}, s, t, statsRegistry)
	ok = ok && checkGauges("p2Requestor", map[string]int{"jobRunning": 0, "jobsWaitingToStart": 0,
		"numRunningTasks": 0, "numWaitingTasks": 0}, s, t, statsRegistry)
	if !ok {
		t.Fatal("failed first requestor gauges test")
	}

	time.Sleep(50 * time.Millisecond) // let prior phase's tasks' threads complete
	s.step()                          // start the next set of tasks
	ok = verifyJobStatus("phase6: verify job3 still running", jobIds[3], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.Completed, domain.Completed, domain.Completed,
			domain.InProgress}, s, t)
	ok = ok && verifyJobStatus("phase6: verify job4 still running", jobIds[4], domain.Completed,
		[]domain.Status{domain.Completed, domain.Completed, domain.Completed, domain.Completed}, s, t)
	ok = ok && verifyJobStatus("phase6: verify job6 waiting to start", jobIds[6], domain.InProgress,
		[]domain.Status{domain.Completed, domain.Completed, domain.Completed, domain.Completed, domain.Completed,
			domain.InProgress}, s, t)

	// check the gauges
	ok = ok && checkGauges("p0Requestor", map[string]int{"jobRunning": 2, "jobsWaitingToStart": 0,
		"numRunningTasks": 2, "numWaitingTasks": 0}, s, t, statsRegistry)
	ok = ok && checkGauges("p1Requestor", map[string]int{"jobRunning": 0, "jobsWaitingToStart": 0,
		"numRunningTasks": 0, "numWaitingTasks": 0}, s, t, statsRegistry)
	ok = ok && checkGauges("p2Requestor", map[string]int{"jobRunning": 0, "jobsWaitingToStart": 0,
		"numRunningTasks": 0, "numWaitingTasks": 0}, s, t, statsRegistry)
	if !ok {
		t.Fatal("failed first requestor gauges test")
	}

	time.Sleep(50 * time.Millisecond) // let prior phase's tasks' threads complete
	s.step()                          // start the next set of tasks
	ok = verifyJobStatus("phase7: verify job3 still running", jobIds[3], domain.Completed,
		[]domain.Status{domain.Completed, domain.Completed, domain.Completed, domain.Completed, domain.Completed,
			domain.Completed}, s, t)
	ok = ok && verifyJobStatus("phase7: verify job6 waiting to start", jobIds[6], domain.Completed,
		[]domain.Status{domain.Completed, domain.Completed, domain.Completed, domain.Completed, domain.Completed,
			domain.Completed}, s, t)

	// check the gauges
	ok = ok && checkGauges("p0Requestor", map[string]int{"jobRunning": 0, "jobsWaitingToStart": 0,
		"numRunningTasks": 0, "numWaitingTasks": 0}, s, t, statsRegistry)
	ok = ok && checkGauges("p1Requestor", map[string]int{"jobRunning": 0, "jobsWaitingToStart": 0,
		"numRunningTasks": 0, "numWaitingTasks": 0}, s, t, statsRegistry)
	ok = ok && checkGauges("p2Requestor", map[string]int{"jobRunning": 0, "jobsWaitingToStart": 0,
		"numRunningTasks": 0, "numWaitingTasks": 0}, s, t, statsRegistry)
	if !ok {
		t.Fatal("failed first requestor gauges test")
	}

	if len(s.requestorsCounts) > 0 {
		t.Fatal("scheduler's requestor count map was not emptied!")
	}

	log.SetLevel(origLevel)
}

func addRequestorTestJobsToScheduler(jobProfiles []map[string]string, s *statefulScheduler) []string {
	jobIds := []string{}
	for _, jobProfile := range jobProfiles {
		numTasks, _ := strconv.Atoi(jobProfile["numTasks"])
		priority, _ := strconv.Atoi(jobProfile["priority"])
		jobId, _, _ := putJobInScheduler(numTasks, s, "complete 0", jobProfile["requestor"],
			domain.Priority(priority))
		s.addJobs()
		jobIds = append(jobIds, jobId)
	}

	return jobIds
}
