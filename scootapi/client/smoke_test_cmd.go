package client

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"time"

	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/scootdev/scoot/tests/testhelpers"
	"github.com/spf13/cobra"
)

type smokeTestCmd struct{}

func (c *smokeTestCmd) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "run_smoke_test",
		Short: "Smoke Test",
	}
	return r
}

func (c *smokeTestCmd) run(cl *Client, cmd *cobra.Command, args []string) error {
	fmt.Println("Starting Smoke Test")

	numJobs := 100

	if (len(args)) > 0 {
		var err error
		numJobs, err = strconv.Atoi(args[0])
		if err != nil {
			return err
		}
	}

	timeout := 180 * time.Second
	if (len(args)) > 1 {
		var err error
		timeout, err = time.ParseDuration(args[1])
		if err != nil {
			return err
		}
	}
	runner := &smokeTestRunner{cl: cl}
	return runner.run(numJobs, timeout)
}

type smokeTestRunner struct {
	cl *Client
}

func (r *smokeTestRunner) run(numJobs int, timeout time.Duration) error {
	jobs := make(map[string]*scoot.JobStatus)

	for i := 0; i < numJobs; i++ {
		id, err := r.generateAndStartJob()
		if err != nil {
			return err
		}
		jobs[id] = nil
	}

	return r.waitForJobs(jobs, timeout)
}

func (r *smokeTestRunner) generateAndStartJob() (string, error) {
	client, err := r.cl.Dial()
	if err != nil {
		return "", err
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// We just want the JobDefinition here Id doesn't matter
	job := testhelpers.GenJobDefinition(rng)
	jobId, err := client.RunJob(job)
	return jobId.ID, err
}

func (r *smokeTestRunner) waitForJobs(jobs map[string]*scoot.JobStatus, timeout time.Duration) error {
	end := time.Now().Add(timeout)
	for {
		printJobs(jobs)
		if time.Now().After(end) {
			return fmt.Errorf("Took longer than %v", timeout)
		}
		done := true
		for k, _ := range jobs {
			d, err := r.updateJobStatus(k, jobs)
			if err != nil {
				return err
			}
			done = done && d
		}
		if done {
			log.Println("Done")
			return nil
		}
		time.Sleep(time.Second)
	}
}

func printJobs(jobs map[string]*scoot.JobStatus) {
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

	log.Println()
	log.Println("Job Status")

	log.Println("Waiting", byStatus[scoot.Status_NOT_STARTED])
	log.Println("Running", byStatus[scoot.Status_IN_PROGRESS])
	log.Println("Done", byStatus[scoot.Status_COMPLETED])
}

func (r *smokeTestRunner) updateJobStatus(jobId string, jobs map[string]*scoot.JobStatus) (bool, error) {
	client, err := r.cl.Dial()
	if err != nil {
		return true, err
	}

	status := jobs[jobId]
	if done(status) {
		return true, nil
	}
	status, err = client.GetStatus(jobId)
	if err != nil {
		return true, err
	}
	jobs[jobId] = status
	return done(status), nil
}

func done(s *scoot.JobStatus) bool {
	return s != nil && (s.Status == scoot.Status_COMPLETED || s.Status == scoot.Status_ROLLED_BACK)
}
