package main

import (
	"flag"
	log "github.com/Sirupsen/logrus"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/scootapi/setup"
)

// Sets up a local swarm that serves scootapi, then waits or runs
func main() {
	schedStrategy := flag.String("strategy", "local.memory", "scheduler/worker strategy to setup")
	apiStrategy := flag.String("api_strategy", "local", "api strategy to setup server w/bundlestore.")
	repoDir := flag.String("repo_dir", "", "backing repo to use as a basis for handling bundles")
	workersFlag := flag.Int("workers", setup.DefaultWorkerCount, "number of workers to use")
	apiserversFlag := flag.Int("apiservers", setup.DefaultApiServerCount, "number of apiservers to use")
	flag.Parse()

	// log.SetFlags(log.LstdFlags | log.LUTC | log.Lshortfile)

	tmp, err := temp.NewTempDir("", "setup-cloud-scoot-")
	if err != nil {
		log.Fatal(err)
	}

	cmds := setup.NewSignalHandlingCmds(tmp)
	defer cmds.Kill()

	builder := setup.NewGoBuilder(cmds)

	workersCfg := &setup.WorkerConfig{Count: *workersFlag, RepoDir: *repoDir}
	sched := map[string]setup.SchedulerStrategy{
		"local.memory": setup.NewLocalMemory(workersCfg, builder, cmds),
		"local.local":  setup.NewLocalLocal(workersCfg, builder, cmds),
	}

	apiCfg := &setup.ApiConfig{Count: *apiserversFlag}
	api := map[string]setup.ApiStrategy{
		"local": setup.NewLocal(apiCfg, builder, cmds),
	}

	strategies := &setup.Strategies{Sched: sched, SchedStrategy: *schedStrategy, Api: api, ApiStrategy: *apiStrategy}
	if err := setup.Main(cmds, strategies, flag.Args()); err != nil {
		cmds.Kill()
		log.Fatal(err)
	}
}
