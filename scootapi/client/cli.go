package client

import (
	"fmt"
	"log"

	"github.com/scootdev/scoot/common/dialer"
	"github.com/scootdev/scoot/scootapi"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
)

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
	// Always recreate a connection.  This way if something went wrong with
	// the previous connection we don't have a clean one.
	return c.createScootClient()
}

// creates a new ScootClient, and sets the property to it
// returns an error if one occured, nil if creating a new client
// and assiging it to c.scootClient was successful
func (c *simpleCLIClient) createScootClient() error {
	if c.addr == "" {
		c.addr = scootapi.GetScootapiAddr()
		if c.addr == "" {
			return fmt.Errorf("scootapi cli addr unset and no valued in %s", scootapi.GetScootapiAddrPath())
		}
		log.Printf("scootapi cli: using addr %v (from %v)", c.addr, scootapi.GetScootapiAddrPath())
	}

	transport, protocolFactory, err := c.dialer.Dial(c.addr)
	if err != nil {
		return fmt.Errorf("Error dialing to set up client connection: %v", err)
	}

	c.scootClient = scoot.NewCloudScootClientFactory(transport, protocolFactory)

	return nil
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
