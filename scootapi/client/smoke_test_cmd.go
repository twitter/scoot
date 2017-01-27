package client

import (
	"fmt"
	"log"
	"time"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/scootapi"
	"github.com/scootdev/scoot/tests/testhelpers"
	"github.com/spf13/cobra"
)

type smokeTestCmd struct {
	numJobs   int
	numTasks  int
	timeout   time.Duration
	storeAddr string
}

func (c *smokeTestCmd) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "run_smoke_test",
		Short: "Smoke Test",
	}
	r.Flags().IntVar(&c.numJobs, "num_jobs", 100, "number of jobs to run")
	r.Flags().IntVar(&c.numTasks, "num_tasks", -1, "number of tasks per job, or random if -1")
	r.Flags().DurationVar(&c.timeout, "timeout", 180*time.Second, "how long to wait for the smoke test")
	r.Flags().StringVar(&c.storeAddr, "bundlestore", "", "address in the form of host:port")

	return r
}

func (c *smokeTestCmd) run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error {
	fmt.Println("Starting Smoke Test")
	fmt.Println("** Note ** Inmemory workers not supported at time since everything they do is a nop.")
	runner := &smokeTestRunner{cl: cl}
	if err := runner.run(c.numJobs, c.numTasks, c.timeout, c.storeAddr); err != nil {
		panic(err) // returning err would make cobra print out usage, which doesn't make sense to do here.
	}
	return nil
}

type smokeTestRunner struct {
	cl *simpleCLIClient
}

func (r *smokeTestRunner) run(numJobs int, numTasks int, timeout time.Duration, storeAddr string) error {
	tmp, err := temp.NewTempDir("", "smoke_test")
	if err != nil {
		return err
	}

	// If store is not specified, test with sim execer. Else generate snapshots and associated commands for use with os execer.
	var cmds []*testhelpers.SnapshotCmd
	if storeAddr == "" {
		_, storeAddr = scootapi.GetScootapiAddr()
	}
	if storeAddr == "" {
		cmds = []*testhelpers.SnapshotCmd{
			testhelpers.DefaultSnapshotCmd(),
		}
	} else {
		if cmds, err = testhelpers.GenerateCmds(tmp, storeAddr, numJobs); err != nil {
			return err
		}
	}

	// Generate the jobs and start executing.
	jobs := make([]string, 0, numJobs)
	jobsToCmds := make(map[string]*testhelpers.SnapshotCmd)
	for i := 0; i < numJobs; i++ {
		for {
			cmd := cmds[i%len(cmds)]
			id, err := testhelpers.GenerateAndStartJob(r.cl.scootClient, numTasks, cmd)
			if err == nil {
				jobs = append(jobs, id)
				jobsToCmds[id] = cmd
				break
			}
			// retry starting job until it succeeds.
			// this is useful for testing where we are restarting the scheduler
			log.Printf("Error Starting Job: Retrying %v", err)
		}
	}

	// Wait for results and then verify that the results are as expected.
	if statuses, err := testhelpers.WaitForJobsToCompleteAndLogStatus(jobs, r.cl.scootClient, timeout); err != nil {
		return err
	} else {
		for jobID, status := range statuses {
			if err = jobsToCmds[jobID].Verify(status); err != nil {
				return err
			}
		}
	}
	return nil
}
