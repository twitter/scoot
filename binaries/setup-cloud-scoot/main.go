package main

import (
	"flag"
	"log"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/scootapi/setup"
)

// Sets up a local swarm that serves scootapi, then waits or runs
func main() {
	strategy := flag.String("strategy", "", "strategy to setup")
	workersFlag := flag.String("workers", "", "strategy-specific flag to configure workers")
	flag.Parse()

	tmp, err := temp.NewTempDir("", "setup-cloud-scoot-")
	if err != nil {
		log.Fatal(err)
	}

	cmds := setup.NewSignalHandlingCmds(tmp)
	defer cmds.Kill()

	builder := setup.NewGoBuilder(cmds)

	strategies := map[string]setup.SchedulerStrategy{
		"local.memory": setup.NewLocalMemory(*workersFlag, builder, cmds),
		"local.local":  setup.NewLocalLocal(*workersFlag, builder, cmds),
	}

	if err := setup.Main(cmds, strategies, *strategy, flag.Args()); err != nil {
		cmds.Kill()
		log.Fatal(err)
	}
}
