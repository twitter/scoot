// Package setup provides Cloud Scoot Setup, which is a mechanism for
// running Scoot with local components, or initializing a connection
// to remote ones, depending on configuration.
package setup

import (
	"fmt"

	"github.com/twitter/scoot/scootapi"
)

type Strategies struct {
	Sched         map[string]SchedulerStrategy
	SchedStrategy string
	Api           map[string]ApiStrategy
	ApiStrategy   string
}

// Main is the entry point for Cloud Scoot setup.
// cmds is running commands
// strategies.sched maps strings to SchedulerStrategy
// strategies.schedStrategy is the name of the strategy to use
// same applies to bundlestore strategy.
// args is the remaining arguments. These can be used to run a command
func Main(cmds *Cmds, strategies *Strategies, args []string) error {
	if err := startup(strategies); err != nil {
		return err
	}

	if len(args) == 0 {
		return wait()
	}
	return run(cmds, args)
}

func startup(strategies *Strategies) error {
	// Startup apiserver, which only incorporates bundlestore for now.
	// Important: do this first since workers run apiserver discovery only once at startup.
	api, ok := strategies.Api[strategies.ApiStrategy]
	if !ok {
		var apiKeys []string
		for k, _ := range strategies.Api {
			apiKeys = append(apiKeys, k)
		}
		return fmt.Errorf("--strategy=%q is not a valid api strategy; valid choices are %v", strategies.ApiStrategy, apiKeys)
	}
	apiAddrs, err := api.Startup()
	if err != nil {
		return err
	}

	// Startup scheduler, and indirectly workers as well.
	sched, ok := strategies.Sched[strategies.SchedStrategy]
	if !ok {
		var schedKeys []string
		for k, _ := range strategies.Sched {
			schedKeys = append(schedKeys, k)
		}
		return fmt.Errorf("--strategy=%q is not a valid sched strategy; valid choices are %v", strategies.SchedStrategy, schedKeys)
	}
	schedAddr, err := sched.Startup()
	if err != nil {
		return err
	}

	// Save the scheduler and first apiserver addresses.
	scootapi.SetScootapiAddr(schedAddr, apiAddrs[0])
	return nil
}

func wait() error {
	select {}
}

func run(cmds *Cmds, args []string) error {
	if args[0] != "run" {
		return fmt.Errorf("command %v is not supported (currently only 'run')", args[0])
	}
	if len(args) < 2 {
		return fmt.Errorf("need at least a command to run; %v", args)
	}
	return cmds.Run(args[1], args[2:]...)
}
