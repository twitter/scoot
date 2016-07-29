package client

import (
	"log"
	"strings"

	"github.com/luci/go-render/render"
	"github.com/scootdev/scoot/workerapi/gen-go/worker"
	"github.com/spf13/cobra"
)

// Run
var runArgv *string
var runcmd *worker.RunCommand = worker.NewRunCommand()

func makeRunCmd(c *Client) *cobra.Command {
	r := &cobra.Command{
		Use:   "run",
		Short: "runs a command",
		RunE:  c.run,
	}
	r.Flags().StringVar(&c.addr, "addr", "localhost:9090", "address to connect to")
	runArgv = r.Flags().String("argv", "", "comma separated list of binary and args")
	runcmd.SnapshotId = r.Flags().String("snapshotid", "", "snapshot/patch id.")
	runcmd.TimeoutMs = r.Flags().Int32("timeout_ms", 0, "timeout before aborting cmd")
	return r
}
func (c *Client) run(cmd *cobra.Command, args []string) error {
	runcmd.Argv = strings.Split(*runArgv, ",")
	log.Println("Calling run rpc to cloud worker", args, render.Render(runcmd))

	client, err := c.Dial()
	if err != nil {
		return err
	}

	status, err := client.Run(runcmd)
	log.Println(render.Render(status))
	return err
}

// Abort
var abortRunId *string

func makeAbortCmd(c *Client) *cobra.Command {
	r := &cobra.Command{
		Use:   "abort",
		Short: "aborts a runId",
		RunE:  c.abort,
	}
	r.Flags().StringVar(&c.addr, "addr", "localhost:9090", "address to connect to")
	abortRunId = r.Flags().String("id", "", "status of a run.")
	return r
}
func (c *Client) abort(cmd *cobra.Command, args []string) error {
	log.Println("Calling abort rpc to cloud worker", args)

	client, err := c.Dial()
	if err != nil {
		return err
	}

	status, err := client.Abort(*abortRunId)
	log.Println(render.Render(status))
	return err
}

// QueryWorker
func makeQueryworkerCmd(c *Client) *cobra.Command {
	r := &cobra.Command{
		Use:   "queryworker",
		Short: "queries worker status",
		RunE:  c.queryworker,
	}
	r.Flags().StringVar(&c.addr, "addr", "localhost:9090", "address to connect to")
	return r
}
func (c *Client) queryworker(cmd *cobra.Command, args []string) error {
	log.Println("Calling queryworker rpc to cloud worker", args)

	client, err := c.Dial()
	if err != nil {
		return err
	}

	status, err := client.QueryWorker()
	log.Println(render.Render(status))
	return err
}

//TODO: implement Erase()
