package client

import (
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/spf13/cobra"
)

type Client struct {
	rootCmd *cobra.Command

	addr   string // populated by flag parsing
	dialer Dialer
	client *scoot.CloudScootClient
}

func (c *Client) Exec() error {
	return c.rootCmd.Execute()
}

func NewClient(dialer Dialer) (*Client, error) {
	r := &Client{}
	r.dialer = dialer

	r.rootCmd = &cobra.Command{
		Use:                "scootapi",
		Short:              "scootapi is a command-line client to Cloud Scoot",
		Run:                func(*cobra.Command, []string) {},
		PersistentPostRunE: r.Close,
	}

	r.addCmd(&runJobCmd{})
	r.addCmd(&getStatusCmd{})
	r.addCmd(&smokeTestCmd{})

	return r, nil
}

func (c *Client) addCmd(cmd cmd) {
	cobraCmd := cmd.registerFlags()
	cobraCmd.Flags().StringVar(&c.addr, "addr", "", "worker server address")
	cobraCmd.RunE = func(innerCmd *cobra.Command, args []string) error {
		return cmd.run(c, innerCmd, args)
	}
	c.rootCmd.AddCommand(cobraCmd)
}

type cmd interface {
	registerFlags() *cobra.Command
	run(cl *Client, cmd *cobra.Command, args []string) error
}
