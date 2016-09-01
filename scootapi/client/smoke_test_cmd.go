package client

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/scootdev/scoot/tests/testhelpers"
	"github.com/spf13/cobra"
	"strconv"
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
	jobs := make(map[string]scoot.Status)
	// run a bunch of concurrent jobs and track their status
	jobIdsCh := make(chan string)
	for i := 0; i < numTasks; i++ {
		id, err := c.generateAndStartJob()
		if err != nil {
			log.Fatal("Error starting job", err)
		}
		jobs[id] = scoot.Status{}
	}

	c.waitForJobs(jobs, timeout)
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

func (c *Client) updateJobStatus(jobId string, jobs map[string]scoot.Status) (done, error) {
	status := jobs[jobId]
	if done(status) {
		return true, nil
	}
	status, err := s.client.GetStatus(jobId)
	if err != nil {
		return true, err
	}
	jobs[jobId] = status
	return done(status), nil
}

func done(s scoot.Status) bool {
	return s.Status == scoot.Status_COMPLETED || s.Status == scoot.Status_ROLLED_BACK
}
