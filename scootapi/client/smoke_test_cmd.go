package client

import (
	"fmt"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/scootdev/scoot/scootapi/testHelpers"
	"github.com/spf13/cobra"
	"math/rand"
	"sync"
	"time"
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

	// run a bunch of concurrent jobs
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			err := c.generateAndRunJob()
			if err != nil {
				fmt.Println(err)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	return nil
}

func (c *Client) generateAndRunJob() error {
	client, err := c.Dial()

	if err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// We just want the JobDefinition here Id doesn't matter
	job := testHelpers.GenJobDefinition(rng)
	jobId, err := client.RunJob(job)

	fmt.Println("Successfully Scheduled Job", jobId.ID)

	// Error Enqueuing Job
	if err != nil {
		switch err := err.(type) {
		case *scoot.InvalidRequest:
			return fmt.Errorf("Invalid Request: %v", err.GetMessage())
		default:
			return fmt.Errorf("Error running job: %v %T", err, err)
		}
	}

	// Check Job Status
	jobInProgress := true
	for jobInProgress {
		status, err := client.GetStatus(jobId.ID)
		if status.Status == scoot.Status_COMPLETED || status.Status == scoot.Status_ROLLED_BACK {
			jobInProgress = false
		}

		if err != nil {
			switch err := err.(type) {
			case *scoot.InvalidRequest:
				return fmt.Errorf("Invalid Request: %v", err.GetMessage())
			case *scoot.ScootServerError:
				return fmt.Errorf("Error getting status: %v", err.Error())
			}
		}

		fmt.Println("Status Update Job", jobId.ID, status.Status)
		time.Sleep(50 * time.Millisecond)
	}

	return nil
}
