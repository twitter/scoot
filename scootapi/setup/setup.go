package setup

import (
	"fmt"

	"github.com/scootdev/scoot/scootapi"
)

// Main is the entry point for Cloud Scoot setup.
// cmds is running commands
// strategies maps strings to SchedulerStrategy
// strategyName is the name of the strategy to use
// args is the remaining arguments. These can be used to run a command
func Main(cmds *Cmds, strategies map[string]SchedulerStrategy, strategyName string, args []string) error {
	if err := startup(strategies, strategyName); err != nil {
		return err
	}

	if len(args) == 0 {
		return wait()
	}
	return run(cmds, args)
}

func startup(strategies map[string]SchedulerStrategy, strategyName string) error {
	var keys []string
	for k, _ := range strategies {
		keys = append(keys, k)
	}

	strategy, ok := strategies[strategyName]
	if !ok {
		return fmt.Errorf("%q is not a valid strategy; valid choices are %v", strategyName, keys)
	}

	addr, err := strategy.Startup()
	if err != nil {
		return err
	}

	scootapi.SetScootapiAddr(addr)
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
