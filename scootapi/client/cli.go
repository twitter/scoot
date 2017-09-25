package client

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/scootapi"
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
	scootClient *scootapi.CloudScootClient
}

func (c *simpleCLIClient) Exec() error {
	return c.rootCmd.Execute()
}

func NewSimpleCLIClient(d dialer.Dialer) (CLIClient, error) {
	c := &simpleCLIClient{}
	c.dial = d

	c.rootCmd = &cobra.Command{
		Use:                "scootapi",
		Short:              "scootapi is a command-line client to Cloud Scoot",
		PersistentPreRunE:  c.Init,
		Run:                func(*cobra.Command, []string) {},
		PersistentPostRunE: c.Close,
	}
	c.rootCmd.PersistentFlags().StringVar(&c.addr, "addr", "", "scoot server address")
	c.rootCmd.PersistentFlags().StringVar(&c.logLevel, "log_level", "info", "Log everything at this level and above (error|info|debug)")

	c.addCmd(&runJobCmd{})
	c.addCmd(&getStatusCmd{})
	c.addCmd(&smokeTestCmd{})
	c.addCmd(&watchJobCmd{})
	c.addCmd(&killJobCmd{})

	return c, nil
}

// Can only be called from cobra command run or hook
func (c *simpleCLIClient) Init(cmd *cobra.Command, args []string) error {
	if c.addr == "" {
		var err error
		c.addr, _, err = scootapi.GetScootapiAddr()
		if err != nil {
			return fmt.Errorf("scootapi cli addr unset and no valued in %s", scootapi.GetScootapiAddrPath())
		}
	}

	level, err := log.ParseLevel(c.logLevel)
	if err != nil {
		log.Error(err)
		return err
	}
	log.SetLevel(level)

	c.scootClient = scootapi.NewCloudScootClient(
		scootapi.CloudScootClientConfig{
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
