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

func makeSmokeTestCmd(c *Client) *cobra.Command {
	r := &cobra.Command{
		Use:   "run_smoke_test",
		Short: "Smoke Test",
		RunE:  c.runSmokeTest,
	}

	r.Flags().StringVar(&c.addr, "addr", "localhost:9090", "address to connect to")
	return r
}

func (c *Client) runSmokeTest(cmd *cobra.Command, args []string) error {
	fmt.Println("Starting Smoke Test")

	numTasks := 100

	if (len(args)) > 0 {
		var err error
		numTasks, err = strconv.Atoi(args[0])
		if err != nil {
			return err
		}
	}

	timeout := 10 * time.Second
	if (len(args)) > 1 {
		var err error
		timeout, err = time.ParseDuration(args[1])
		if err != nil {
			return err
		}
	}
	jobs := make(map[string]*scoot.JobStatus)
	for i := 0; i < numTasks; i++ {
		id, err := c.generateAndStartJob()
		if err != nil {
			return err
		}
		jobs[id] = nil
	}

	return c.waitForJobs(jobs, timeout)
}

func (c *Client) generateAndStartJob() (string, error) {
	client, err := c.Dial()
	if err != nil {
		return "", err
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// We just want the JobDefinition here Id doesn't matter
	job := testhelpers.GenJobDefinition(rng)
	jobId, err := client.RunJob(job)
	return jobId.ID, err
}

func (c *Client) waitForJobs(jobs map[string]*scoot.JobStatus, timeout time.Duration) error {
	end := time.Now().Add(timeout)
	for {
		printJobs(jobs)
		if time.Now().After(end) {
			return fmt.Errorf("Took longer than %v", timeout)
		}
		done := true
		for k, _ := range jobs {
			d, err := c.updateJobStatus(k, jobs)
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

func (c *Client) updateJobStatus(jobId string, jobs map[string]*scoot.JobStatus) (bool, error) {
	client, err := c.Dial()
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
