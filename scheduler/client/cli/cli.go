package cli

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/scheduler/api/thrift/gen-go/scoot"
	"github.com/twitter/scoot/scheduler/client"
)

// Scoot API Client interface that includes CLI handling
type CLIClient interface {
	Exec() error
}

// Implements CLIClient - basic
type simpleCLIClient struct {
	rootCmd     *cobra.Command
	addr        string
	dial        dialer.Dialer
	logLevel    string
	scootClient *client.CloudScootClient
}

// returnError extend the error with Invalid Request, Scoot server error, or Error getting status message
func returnError(err error) error {
	switch err := err.(type) {
	case *scoot.InvalidRequest:
		return fmt.Errorf("Invalid Request: %v", err.GetMessage())
	case *scoot.ScootServerError:
		return fmt.Errorf("Scoot server error: %v", err.Error())
	default:
		return fmt.Errorf("Error getting status: %v", err.Error())
	}
}

func (c *simpleCLIClient) Exec() error {
	return c.rootCmd.Execute()
}

func NewSimpleCLIClient(d dialer.Dialer) (CLIClient, error) {
	c := &simpleCLIClient{}
	c.dial = d

	c.rootCmd = &cobra.Command{
		Use:                "scootcl",
		Short:              "scootcl is a command-line client to Scoot",
		PersistentPreRunE:  c.Init,
		Run:                func(*cobra.Command, []string) {},
		PersistentPostRunE: c.Close,
	}
	sched, _, _ := client.GetScootapiAddr() // ignore err & apiserver addr
	c.rootCmd.PersistentFlags().StringVar(&c.addr, "addr", sched, "Scoot server address. If unset, uses default value of first line of $HOME/.cloudscootaddr$SCOOT_ID")
	c.rootCmd.PersistentFlags().StringVar(&c.logLevel, "log_level", "info", "Log everything at this level and above (error|info|debug)")

	c.addCmd(&runJobCmd{})
	c.addCmd(&getStatusCmd{})
	c.addCmd(&smokeTestCmd{})
	c.addCmd(&watchJobCmd{})
	c.addCmd(&killJobCmd{})
	c.addCmd(&offlineWorkerCmd{})
	c.addCmd(&reinstateWorkerCmd{})
	c.addCmd(&setSchedulerStatusCmd{})
	c.addCmd(&getSchedulerStatusCmd{})
	c.addCmd(&getLBSSchedAlgParams{})
	c.addCmd(&setLbsSchedAlgParams{})

	return c, nil
}

// Can only be called from cobra command run or hook
func (c *simpleCLIClient) Init(cmd *cobra.Command, args []string) error {
	if c.addr == "" {
		var err error
		c.addr, _, err = client.GetScootapiAddr()
		if err != nil {
			return fmt.Errorf("scootapi cli addr unset and no valued in %s", client.GetScootapiAddrPath())
		}
	}

	level, err := log.ParseLevel(c.logLevel)
	if err != nil {
		log.Error(err)
		return err
	}
	log.SetLevel(level)

	c.scootClient = client.NewCloudScootClient(
		client.CloudScootClientConfig{
			Addr:   c.addr,
			Dialer: c.dial,
		})

	return nil
}

// Needs cobra parameters for use from rootCmd
func (c *simpleCLIClient) Close(cmd *cobra.Command, args []string) error {
	if c.scootClient != nil {
		return c.scootClient.Close()
	}
	return nil
}

func (c *simpleCLIClient) addCmd(cmd command) {
	cobraCmd := cmd.registerFlags()
	cobraCmd.RunE = func(innerCmd *cobra.Command, args []string) error {
		return cmd.run(c, innerCmd, args)
	}
	c.rootCmd.AddCommand(cobraCmd)
}

type command interface {
	registerFlags() *cobra.Command
	run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error
}
