package client

import (
	"fmt"

	"github.com/scootdev/scoot/common/dialer"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
)

const defaultSchedulerAddr = "localhost:9090"

// Scoot API Client interface that includes CLI handling
type CLIClient interface {
	Exec() error
}

// Implements CLIClient - basic
type simpleCLIClient struct {
	rootCmd *cobra.Command

	addr        string
	dialer      dialer.Dialer
	scootClient *scoot.CloudScootClient
}

func (c *simpleCLIClient) Exec() error {
	return c.rootCmd.Execute()
}

func NewSimpleCLIClient(d dialer.Dialer) (CLIClient, error) {
	c := &simpleCLIClient{}
	c.dialer = d
	// c.addr is populated by flag

	c.rootCmd = &cobra.Command{
		Use:                "scootapi",
		Short:              "scootapi is a command-line client to Cloud Scoot",
		Run:                func(*cobra.Command, []string) {},
		PersistentPostRunE: c.Close,
	}

	c.addCmd(&runJobCmd{})
	c.addCmd(&getStatusCmd{})
	c.addCmd(&smokeTestCmd{})
	c.addCmd(&watchJobCmd{})

	return c, nil
}

func (c *simpleCLIClient) Dial() error {
	_, err := c.dial()
	return err
}

func (c *simpleCLIClient) dial() (*scoot.CloudScootClient, error) {
	if c.scootClient == nil {
		if c.addr == "" {
			c.addr = defaultSchedulerAddr
		}

		transport, protocolFactory, err := c.dialer.Dial(c.addr)
		if err != nil {
			return nil, fmt.Errorf("Error dialing to set up client connection: %v", err)
		}

		c.scootClient = scoot.NewCloudScootClientFactory(transport, protocolFactory)
	}
	return c.scootClient, nil
}

// Needs cobra parameters for use from rootCmd
func (c *simpleCLIClient) Close(cmd *cobra.Command, args []string) error {
	if c.scootClient != nil {
		return c.scootClient.Transport.Close()
	}
	return nil
}

func (c *simpleCLIClient) addCmd(cmd command) {
	cobraCmd := cmd.registerFlags()
	cobraCmd.Flags().StringVar(&c.addr, "addr", "", "scoot server address")
	cobraCmd.RunE = func(innerCmd *cobra.Command, args []string) error {
		return cmd.run(c, innerCmd, args)
	}
	c.rootCmd.AddCommand(cobraCmd)
}

type command interface {
	registerFlags() *cobra.Command
	run(cl *simpleCLIClient, cmd *cobra.Command, args []string) error
}
