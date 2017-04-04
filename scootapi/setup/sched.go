package setup

import (
	log "github.com/Sirupsen/logrus"
	"strconv"
	"strings"

	"github.com/scootdev/scoot/scootapi"
)

// SchedulerStrategy will startup a Scheduler (or setup a connection to one)
type SchedulerStrategy interface {

	// Startup starts up a Scheduler, returning the address of the server or an error
	Startup() (string, error)
}

// LocalSchedStrategy starts up a local scheduler
type LocalSchedStrategy struct {
	workersCfg *WorkerConfig
	workers    WorkersStrategy
	builder    Builder
	cmds       *Cmds
	logLevel   log.Level
}

// Create a new Local Scheduler that will talk to workers, using builder and cmds to start
func NewLocalSchedStrategy(workersCfg *WorkerConfig, workers WorkersStrategy, builder Builder, cmds *Cmds, level log.Level) *LocalSchedStrategy {
	return &LocalSchedStrategy{
		workersCfg: workersCfg,
		workers:    workers,
		builder:    builder,
		cmds:       cmds,
		logLevel:   level,
	}
}

func (s *LocalSchedStrategy) Startup() (string, error) {
	log.Info("Starting up a Local Scheduler")

	config, err := s.workers.StartupWorkers()
	if err != nil {
		return "", err
	}

	bin, err := s.builder.Scheduler()
	if err != nil {
		return "", err
	}

	if err := s.cmds.Start(bin,
		"-thrift_addr", scootapi.DefaultSched_Thrift,
		"-http_addr", scootapi.DefaultSched_HTTP,
		"-log_level", s.logLevel.String(),
		"-config", config); err != nil {
		return "", err
	}

	thriftPort, _ := strconv.Atoi(strings.Split(scootapi.DefaultSched_Thrift, ":")[1])
	httpPort, _ := strconv.Atoi(strings.Split(scootapi.DefaultSched_HTTP, ":")[1])
	if err := WaitForPort(thriftPort); err != nil {
		return "", err
	}
	if err := WaitForPort(httpPort); err != nil {
		return "", err
	}

	return scootapi.DefaultSched_Thrift, nil
}

// Create a SchedulerStrategy with a local scheduler and in-memory workers
func NewLocalMemory(workersCfg *WorkerConfig, builder Builder, cmds *Cmds, logLevel log.Level) *LocalSchedStrategy {
	return NewLocalSchedStrategy(workersCfg, NewInMemoryWorkers(workersCfg, logLevel), builder, cmds, logLevel)
}

// Create a SchedulerStrategy with a local scheduler and local workers
func NewLocalLocal(workersCfg *WorkerConfig, builder Builder, cmds *Cmds, logLevel log.Level) *LocalSchedStrategy {
	return NewLocalSchedStrategy(workersCfg, NewLocalWorkers(workersCfg, builder, cmds, logLevel), builder, cmds, logLevel)
}
