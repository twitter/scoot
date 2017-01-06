package client

import (
	"fmt"

	"github.com/scootdev/scoot/common/dialer"
	"github.com/scootdev/scoot/scootapi"
	"github.com/spf13/cobra"
)

// Scoot API Client interface that includes CLI handling
type CLIClient interface {
	Exec() error
}

// Implements CLIClient - basic
type simpleCLIClient struct {
	rootCmd     *cobra.Command
	addr        string
	scootClient *scootapi.CloudScootClient
}

func (c *simpleCLIClient) Exec() error {
	return c.rootCmd.Execute()
}

func NewSimpleCLIClient(d dialer.Dialer) (CLIClient, error) {
	c := &simpleCLIClient{}

	c.rootCmd = &cobra.Command{
		Use:                "scootapi",
		Short:              "scootapi is a command-line client to Cloud Scoot",
		Run:                func(*cobra.Command, []string) {},
		PersistentPostRunE: c.Close,
	}

	// c.addr is populated by flag
	if c.addr == "" {
		c.addr = scootapi.GetScootapiAddr()
		if c.addr == "" {
			return c, fmt.Errorf("scootapi cli addr unset and no valued in %s", scootapi.GetScootapiAddrPath())
		}
	}

	c.scootClient = scootapi.NewCloudScootClient(
		scootapi.CloudScootClientConfig{
			Addr:   c.addr,
			Dialer: d,
		})

	c.addCmd(&runJobCmd{})
	c.addCmd(&getStatusCmd{})
	c.addCmd(&smokeTestCmd{})
	c.addCmd(&watchJobCmd{})

	return c, nil
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
