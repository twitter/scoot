package cli

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	commoncli "github.com/twitter/scoot/common/client"
	"github.com/twitter/scoot/common/dialer"
	smoketest "github.com/twitter/scoot/integration-tests/smoketest"
	"github.com/twitter/scoot/scheduler/api/thrift/gen-go/scoot"
	"github.com/twitter/scoot/scheduler/client"
)

// SchedCLIClient includes fields required for CLI client handling
type SchedCLIClient struct {
	commoncli.SimpleClient
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

func (c *SchedCLIClient) Exec() error {
	return c.RootCmd.Execute()
}

func NewSimpleCLIClient(d dialer.Dialer) (commoncli.CLIClient, error) {
	c := &SchedCLIClient{}
	c.Dial = d

	c.RootCmd = &cobra.Command{
		Use:                "scootcl",
		Short:              "scootcl is a command-line client to Scoot",
		PersistentPreRunE:  c.Init,
		Run:                func(*cobra.Command, []string) {},
		PersistentPostRunE: c.Close,
	}
	sched, _, _ := client.GetScootapiAddr() // ignore err & apiserver addr
	c.RootCmd.PersistentFlags().StringVar(&c.Addr, "addr", sched, "Scoot server address. If unset, uses default value of first line of $HOME/.cloudscootaddr$SCOOT_ID")
	c.RootCmd.PersistentFlags().StringVar(&c.LogLevel, "log_level", "info", "Log everything at this level and above (error|info|debug)")

	c.addCmd(&runJobCmd{})
	c.addCmd(&getStatusCmd{})
	c.addCmd(&smoketest.SmokeTestCmd{})
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
func (c *SchedCLIClient) Init(cmd *cobra.Command, args []string) error {
	if c.Addr == "" {
		var err error
		c.Addr, _, err = client.GetScootapiAddr()
		if err != nil {
			return fmt.Errorf("scootapi cli addr unset and no valued in %s", client.GetScootapiAddrPath())
		}
	}

	level, err := log.ParseLevel(c.LogLevel)
	if err != nil {
		log.Error(err)
		return err
	}
	log.SetLevel(level)

	c.ScootClient = client.NewCloudScootClient(
		client.CloudScootClientConfig{
			Addr:   c.Addr,
			Dialer: c.Dial,
		})

	return nil
}

// Needs cobra parameters for use from rootCmd
func (c *SchedCLIClient) Close(cmd *cobra.Command, args []string) error {
	if c.ScootClient != nil {
		return c.ScootClient.Close()
	}
	return nil
}

func (c *SchedCLIClient) addCmd(cmd commoncli.Cmd) {
	cobraCmd := cmd.RegisterFlags()
	cobraCmd.RunE = func(innerCmd *cobra.Command, args []string) error {
		return cmd.Run(&c.SimpleClient, innerCmd, args)
	}
	c.RootCmd.AddCommand(cobraCmd)
}
