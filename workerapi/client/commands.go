package client

import (
	"log"
	"strings"
	"time"

	"github.com/luci/go-render/render"
	"github.com/scootdev/scoot/runner"
	"github.com/spf13/cobra"
)

// Run
var runArgv *string
var runcmd *runner.Command = &runner.Command{}

func makeRunCmd(c *cliClient) *cobra.Command {
	r := &cobra.Command{
		Use:   "run",
		Short: "runs a command",
		RunE:  c.run,
	}
	r.Flags().StringVar(&c.client.addr, "addr", "localhost:9090", "address to connect to")
	runArgv = r.Flags().String("argv", "", "comma separated list of binary and args")
	runcmd.SnapshotId = *r.Flags().String("snapshotid", "", "snapshot/patch id.")
	runcmd.Timeout = time.Duration(*r.Flags().Int32("timeout_ms", 0, "timeout to abort cmd")) * time.Millisecond
	return r
}
func (c *cliClient) run(cmd *cobra.Command, args []string) error {
	runcmd.Argv = strings.Split(*runArgv, ",")
	log.Println("Calling run rpc to cloud worker", args, render.Render(runcmd))

	status, err := c.client.Run(runcmd)
	log.Println(render.Render(status), err)
	return nil
}

// Abort
var abortRunId *string

func makeAbortCmd(c *cliClient) *cobra.Command {
	r := &cobra.Command{
		Use:   "abort",
		Short: "aborts a runId",
		RunE:  c.abort,
	}
	r.Flags().StringVar(&c.client.addr, "addr", "localhost:9090", "address to connect to")
	abortRunId = r.Flags().String("id", "", "status of a run.")
	return r
}
func (c *cliClient) abort(cmd *cobra.Command, args []string) error {
	log.Println("Calling abort rpc to cloud worker", args)

	status, err := c.client.Abort(runner.RunId(*abortRunId))
	log.Println(render.Render(status), err)
	return nil
}

// QueryWorker
func makeQueryworkerCmd(c *cliClient) *cobra.Command {
	r := &cobra.Command{
		Use:   "queryworker",
		Short: "queries worker status",
		RunE:  c.queryworker,
	}
	r.Flags().StringVar(&c.client.addr, "addr", "localhost:9090", "address to connect to")
	return r
}
func (c *cliClient) queryworker(cmd *cobra.Command, args []string) error {
	log.Println("Calling queryworker rpc to cloud worker", args)

	status, err := c.client.Status()
	log.Println(render.Render(status), err)
	return nil
}

//TODO: implement Erase()
