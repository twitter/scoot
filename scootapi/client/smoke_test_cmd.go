package client

import (
	"fmt"
	"log"
	"strconv"
	"time"

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

func (c *smokeTestCmd) run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error {
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
	cl *simpleCLIClient
}

func (r *smokeTestRunner) run(numJobs int, timeout time.Duration) error {
	jobs := make([]string, 0, numJobs)

	for i := 0; i < numJobs; i++ {
		id, err := testhelpers.GenerateAndStartJob(r.cl.scootClient)
		// retry starting job until it succeeds

		for err != nil {
			log.Printf("Error Starting Job: Retrying %v", err)
			id, err = testhelpers.GenerateAndStartJob(r.cl.scootClient)
		}
		jobs = append(jobs, id)
	}

	return testhelpers.WaitForJobsToCompleteAndLogStatus(
		jobs, r.cl.scootClient, timeout)
}
