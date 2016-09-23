package client

import (
	"fmt"
	"github.com/scootdev/scoot/common/thrifthelpers"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
	"io/ioutil"
	"log"
	"os"
)

type testTargetsCmd struct {
	jobFilePath string
}

func (t *testTargetsCmd) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "test_targets",
		Short: "test targets",
	}
	r.Flags().StringVar(&t.jobFilePath, "job_file_path", "", "file to read targets from")

	return r
}

func (t *testTargetsCmd) run(cl *Client, cmd *cobra.Command, args []string) error {
	log.Println("Testing targets on scoot")

	client, err := cl.Dial()
	if err != nil {
		return err
	}

	// translate file into scoot jobDef
	f, err := os.Open(t.jobFilePath)
	asBytes, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}

	jobDef := scoot.NewJobDefinition()
	thrifthelpers.JsonDeserialize(jobDef, asBytes)

	_, err = client.RunJob(jobDef)
	if err != nil {
		switch err := err.(type) {
		case *scoot.InvalidRequest:
			return fmt.Errorf("Invalid Request: %v", err.GetMessage())
		default:
			return fmt.Errorf("Error running job: %v %T", err, err)
		}
	}
	return nil
}
