package testhelpers

import (
	"fmt"
	"math/rand"
	"sort"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/scootapi"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
)

// Creates a CloudScootClient that talks to the specified address
func CreateScootClient(addr string) *scootapi.CloudScootClient {
	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	di := dialer.NewSimpleDialer(transportFactory, protocolFactory, time.Minute)

	scootClient := scootapi.NewCloudScootClient(
		scootapi.CloudScootClientConfig{
			Addr:   addr,
			Dialer: di,
		})

	return scootClient
}

// Generates a random Job and sends it to the specified client to run
// returns the JobId if successfully scheduled, otherwise "", error
func GenerateJob(numTasks int, snapshotID string) *scoot.JobDefinition {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	jobDef := GenJobDefinition(rng, numTasks, snapshotID)
	return jobDef
}

func StartJob(client *scootapi.CloudScootClient, job *scoot.JobDefinition) string {
	for {
		j, err := client.RunJob(job)
		if err == nil {
			return j.ID
		}
		// retry starting job until it succeeds.
		// this is useful for testing where we are restarting the scheduler
		log.Infof("Error Starting Job: Retrying %v", err)
	}
}

// Waits until all jobs specified have completed running or the
// specified timeout has occurred.  Periodically the status of
// running jobs is printed to the console
func WaitForJobsToCompleteAndLogStatus(
	jobIds []string,
	client scoot.CloudScoot,
	timeout time.Duration,
) error {

	jobs := make(map[string]*scoot.JobStatus)
	for _, id := range jobIds {
		jobs[id] = nil
	}

	end := time.Now().Add(timeout)
	for {
		if time.Now().After(end) {
			return fmt.Errorf("Took longer than %v", timeout)
		}
		done := true

		for jobId, oldStatus := range jobs {

			if !IsJobCompleted(oldStatus) {
				currStatus, err := client.GetStatus(jobId)

				// if there is an error just continue
				if err != nil {
					log.Infof("Error: Updating Job Status ID: %v will retry later, Error: %v", jobId, err)
					done = false
				} else {
					jobs[jobId] = currStatus
					done = done && IsJobCompleted(currStatus)
				}
			}
		}
		PrintJobs(jobs)
		if done {
			log.Info("Done")
			return nil
		}
		time.Sleep(time.Second)
	}
}

// Show job progress in the format <jobId> (<done>/<total>), e.g. ffb16fef-13fd-486c-6070-8df9c7b80dce (9997/10000)
type jobProgress struct {
	id       string
	numDone  int
	numTasks int
}

func (p jobProgress) String() string { return fmt.Sprintf("%s (%d/%d)", p.id, p.numDone, p.numTasks) }

// Prints the current status of the specified Jobs to the Log
func PrintJobs(jobs map[string]*scoot.JobStatus) {
	byStatus := make(map[scoot.Status][]string)
	for k, v := range jobs {
		st := scoot.Status_NOT_STARTED
		if v != nil {
			st = v.Status
		}
		byStatus[st] = append(byStatus[st], k)
	}

	for _, v := range byStatus {
		sort.Sort(sort.StringSlice(v))
	}

	inProgress := byStatus[scoot.Status_IN_PROGRESS]
	progs := make([]jobProgress, len(inProgress))
	for i, jobID := range inProgress {
		jobStatus := jobs[jobID]
		tasks := jobStatus.TaskStatus
		numDone := 0
		for _, st := range tasks {
			if st == scoot.Status_COMPLETED {
				numDone++
			}
		}
		progs[i] = jobProgress{id: jobID, numTasks: len(tasks), numDone: numDone}
	}

	log.Info()
	log.Info("Job Status")

	log.Info("Waiting", byStatus[scoot.Status_NOT_STARTED])
	log.Info("Running", progs)
	log.Info("Done", byStatus[scoot.Status_COMPLETED])
}

// Returns true if a job is completed or failed, false otherwise
func IsJobCompleted(s *scoot.JobStatus) bool {
	return s != nil && (s.Status == scoot.Status_COMPLETED || s.Status == scoot.Status_ROLLED_BACK)
}
