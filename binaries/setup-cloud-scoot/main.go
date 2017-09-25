package main

import (
	"flag"
	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/scootapi/setup"
)

// Sets up a local swarm that serves scootapi, then waits or runs
func main() {
	log.AddHook(hooks.NewContextHook())
	schedStrategy := flag.String("strategy", "local.memory", "scheduler/worker strategy to setup")
	apiStrategy := flag.String("api_strategy", "local", "api strategy to setup server w/bundlestore.")
	repoDir := flag.String("repo_dir", "", "backing repo to use as a basis for handling bundles")
	workersFlag := flag.Int("workers", setup.DefaultWorkerCount, "number of workers to use")
	apiserversFlag := flag.Int("apiservers", setup.DefaultApiServerCount, "number of apiservers to use")
	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
	flag.Parse()
	level, err := log.ParseLevel(*logLevelFlag)
	if err != nil {
		log.Error(err)
		return
	}
	log.SetLevel(level)

	tmp, err := temp.NewTempDir("", "setup-cloud-scoot-")
	if err != nil {
		log.Fatal(err)
	}

	cmds := setup.NewSignalHandlingCmds(tmp)
	defer cmds.Kill()

	builder := setup.NewGoBuilder(cmds)

	workersCfg := &setup.WorkerConfig{Count: *workersFlag, RepoDir: *repoDir, LogLevel: level}
	sched := map[string]setup.SchedulerStrategy{
		"local.memory": setup.NewLocalMemory(workersCfg, builder, cmds),
		"local.local":  setup.NewLocalLocal(workersCfg, builder, cmds),
	}

	apiCfg := &setup.ApiConfig{Count: *apiserversFlag, LogLevel: level}
	api := map[string]setup.ApiStrategy{
		"local": setup.NewLocal(apiCfg, builder, cmds),
	}

	strategies := &setup.Strategies{Sched: sched, SchedStrategy: *schedStrategy, Api: api, ApiStrategy: *apiStrategy}
	if err := setup.Main(cmds, strategies, flag.Args()); err != nil {
		cmds.Kill()
		log.Fatal(err)
	}
}
