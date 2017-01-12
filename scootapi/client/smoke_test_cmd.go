package client

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/scootdev/scoot/tests/testhelpers"
	"github.com/spf13/cobra"
)

type smokeTestCmd struct {
	numJobs  int
	numTasks int
	timeout  time.Duration
}

func (c *smokeTestCmd) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "run_smoke_test",
		Short: "Smoke Test",
	}
	r.Flags().IntVar(&c.numJobs, "num_jobs", 100, "number of jobs to run")
	r.Flags().IntVar(&c.numTasks, "num_tasks", -1, "number of tasks per job")
	r.Flags().DurationVar(&c.timeout, "timeout", 180*time.Second, "how long to wait for the smoke test")
	return r
}

func (c *smokeTestCmd) run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error {
	fmt.Println("Starting Smoke Test")

	// TODO(dbentley): migrate away from positional args to flags
	if (len(args)) > 0 {
		numJobs, err := strconv.Atoi(args[0])
		if err != nil {
			return err
		}
		c.numJobs = numJobs
	}

	if (len(args)) > 1 {
		timeout, err := time.ParseDuration(args[1])
		if err != nil {
			return err
		}
		c.timeout = timeout
	}
	runner := &smokeTestRunner{cl: cl}
	return runner.run(c.numJobs, c.numTasks, c.timeout)
}

type smokeTestRunner struct {
	cl *simpleCLIClient
}

func (r *smokeTestRunner) run(numJobs int, numTasks int, timeout time.Duration) error {
	jobs := make([]string, 0, numJobs)

	for i := 0; i < numJobs; i++ {
		for {
			id, err := testhelpers.GenerateAndStartJob(r.cl.scootClient, numTasks)
			if err == nil {
				jobs = append(jobs, id)
				break
			}
			log.Printf("Error Starting Job: Retrying %v", err)
		}
	}
	return testhelpers.WaitForJobsToCompleteAndLogStatus(
		jobs, r.cl.scootClient, timeout)
}
