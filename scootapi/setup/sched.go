package setup

import (
	"log"
)

// SchedulerStrategy will startup a Scheduler (or setup a connection to one)
type SchedulerStrategy interface {

	// Startup starts up a Scheduler, returing the address of the server or an error
	Startup() (string, error)
}

// LocalSchedStrategy starts up a local scheduler
type LocalSchedStrategy struct {
	workersCfg *WorkerConfig
	workers    WorkersStrategy
	builder    Builder
	cmds       *Cmds
}

// Create a new Local Scheduler that will talk to workers, using builder and cmds to start
func NewLocalSchedStrategy(workersCfg *WorkerConfig, workers WorkersStrategy, builder Builder, cmds *Cmds) *LocalSchedStrategy {
	return &LocalSchedStrategy{
		workersCfg: workersCfg,
		workers:    workers,
		builder:    builder,
		cmds:       cmds,
	}
}

func (s *LocalSchedStrategy) Startup() (string, error) {
	log.Println("Starting up a Local Scheduler")

	config, err := s.workers.StartupWorkers()
	if err != nil {
		return "", err
	}

	bin, err := s.builder.Scheduler()
	if err != nil {
		return "", err
	}

	if err := s.cmds.Start(bin, "-config", config, "-repo", s.workersCfg.RepoDir, "-bundlestore", s.workersCfg.StoreHandle); err != nil {
		return "", err
	}

	if err := WaitForPort("9090"); err != nil {
		return "", err
	}

	return "localhost:9090", nil
}

// Create a SchedulerStrategy with a local scheduler and in-memory workers
func NewLocalMemory(workersCfg *WorkerConfig, builder Builder, cmds *Cmds) *LocalSchedStrategy {
	return NewLocalSchedStrategy(workersCfg, NewInMemoryWorkers(workersCfg), builder, cmds)
}

// Create a SchedulerStrategy with a local scheduler and local workers
func NewLocalLocal(workersCfg *WorkerConfig, builder Builder, cmds *Cmds) *LocalSchedStrategy {
	return NewLocalSchedStrategy(workersCfg, NewLocalWorkers(workersCfg, builder, cmds), builder, cmds)
}
