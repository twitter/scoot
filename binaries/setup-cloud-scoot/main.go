package main

import (
	"flag"
	"log"
	"strconv"
	"strings"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/scootapi/setup"
)

// Sets up a local swarm that serves scootapi, then waits or runs
func main() {
	strategy := flag.String("strategy", "", "strategy to setup")
	workersFlag := flag.String("workers", "", "strategy-specific flag to configure workers, ex: --workers='--repoDir=/tmp <COUNT>'")
	flag.Parse()

	// Extract flags associated with worker.
	//NOTE: can't parse quoted spaces in the workersFlag string.
	//TODO: gitdb currently checks out commit snapshots in-place which won't work for multiple workers. It needs to not be in-place.
	//      For now, if repo is specified, callers must only use ingested dirs, not commits, as snapshots. (i.e. gitdb.IngestDir())
	//TODO: option to start a store server and run against that.
	wf := flag.NewFlagSet("workerFlags", flag.ExitOnError)
	repoDir := wf.String("repo", "", "If starting workers: abs dir path to a git repo to run against (don't use important repos yet!).")
	storeHandle := wf.String("bundlestore", "", "If starting workers: abs file path or http URL where repo uploads/downloads bundles.")
	wf.Parse(strings.Split(strings.Trim(*workersFlag, " "), " "))
	count := setup.DefaultWorkerCount
	if wf.Arg(0) != "" {
		var err error
		if count, err = strconv.Atoi(wf.Arg(0)); err != nil {
			log.Fatal("--workers takes a worker count >= 0: ", err)
		}
	}
	workersCfg := &setup.WorkerConfig{Count: count, RepoDir: *repoDir, StoreHandle: *storeHandle}

	tmp, err := temp.NewTempDir("", "setup-cloud-scoot-")
	if err != nil {
		log.Fatal(err)
	}

	cmds := setup.NewSignalHandlingCmds(tmp)
	defer cmds.Kill()

	builder := setup.NewGoBuilder(cmds)

	strategies := map[string]setup.SchedulerStrategy{
		"local.memory": setup.NewLocalMemory(workersCfg, builder, cmds),
		"local.local":  setup.NewLocalLocal(workersCfg, builder, cmds),
	}

	if err := setup.Main(cmds, strategies, *strategy, flag.Args()); err != nil {
		cmds.Kill()
		log.Fatal(err)
	}
}
