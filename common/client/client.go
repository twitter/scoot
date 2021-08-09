package client

import (
	"github.com/spf13/cobra"

	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/scheduler/client"
)

// Client interface that includes CLI handling
type CLIClient interface {
	Exec() error
}

// SimpleClient includes base fields required for implementing client
type SimpleClient struct {
	RootCmd     *cobra.Command
	Addr        string
	Dial        dialer.Dialer
	LogLevel    string
	ScootClient *client.CloudScootClient
}

// Command interface used to run client commands
type Cmd interface {
	RegisterFlags() *cobra.Command
	Run(cl *SimpleClient, cmd *cobra.Command, args []string) error
}
