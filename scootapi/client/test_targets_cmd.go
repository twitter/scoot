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
	filePath string
}

func (t *testTargetsCmd) registerFlags() *cobra.Command {
	r := &cobra.Command{
		Use:   "test_targets",
		Short: "test targets",
	}
	r.Flags().StringVar(&t.filePath, "file_path", "", "file to read targets from")

	return r
}

func (t *testTargetsCmd) run(cl *Client, cmd *cobra.Command, args []string) error {
	log.Println("Running on scoot", args)

	client, err := cl.Dial()
	if err != nil {
		return err
	}
	// translate read file into domain scoot job to be run
	f, err := os.Open(t.filePath)
	asBytes, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	fmt.Println("FILEEEE")
	fmt.Println(asBytes)

	jobDef := scoot.NewJobDefinition()
	err = thrifthelpers.BinaryDeserialize(jobDef, asBytes)
	if err != nil {
		return nil
	}

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
