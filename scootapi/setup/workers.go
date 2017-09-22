package setup

import (
	"fmt"
	"strconv"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/scootapi"
)

const DefaultWorkerCount int = 5
const DefaultWorkerLogLevel log.Level = log.InfoLevel

// WorkersStrategy is a strategy to start workers and returns the config to pass to a scheduler to talk to them
type WorkersStrategy interface {
	// StartupWorkers should startup necessary workers, and returns the config to pass to Scheduler, or an error
	StartupWorkers() (string, error)
}

// In addition to count, we'll optionally want repoDir to initialize workers' gitdb.
// Whatever is unset will be given a default value.
// We also set a logLevel, which determines the minimum level
// to display in scheduler/worker logs.
type WorkerConfig struct {
	Count    int
	RepoDir  string
	LogLevel log.Level
}

// InMemoryWorkersStrategy will use in-memory workers (to test the Scheduler logic)
type InMemoryWorkersStrategy struct {
	workersCfg *WorkerConfig
}

// NewInMemoryWorkers creates a new InMemoryWorkersStartup
func NewInMemoryWorkers(workersCfg *WorkerConfig) *InMemoryWorkersStrategy {
	if workersCfg == nil {
		workersCfg = &WorkerConfig{}
	}
	return &InMemoryWorkersStrategy{workersCfg: workersCfg}
}

func (s *InMemoryWorkersStrategy) StartupWorkers() (string, error) {
	log.Info("Using in-memory workers")
	if s.workersCfg.Count == 0 {
		s.workersCfg.Count = 10
	}
	// A log level of 0 corresponds to Panic. Default behavior shouldn't be to surpress all log output
	// besides log.Panic, so we set a default of Info.
	if s.workersCfg.LogLevel <= 0 {
		s.workersCfg.LogLevel = DefaultWorkerLogLevel
	}
	return fmt.Sprintf(`local.memory.{"Cluster": {"Count": %d}}`, s.workersCfg.Count), nil
}

// LocalWorkersStrategy will startup workers running locally
type LocalWorkersStrategy struct {
	workersCfg *WorkerConfig
	builder    Builder
	cmds       *Cmds
	nextPort   int
}

// NewLocalWorkers creates a new LocalWorkersStartup
func NewLocalWorkers(workersCfg *WorkerConfig, builder Builder, cmds *Cmds) *LocalWorkersStrategy {
	if workersCfg == nil {
		workersCfg = &WorkerConfig{}
	}
	return &LocalWorkersStrategy{
		workersCfg: workersCfg,
		builder:    builder,
		cmds:       cmds,
		nextPort:   scootapi.WorkerPorts,
	}
}

func (s *LocalWorkersStrategy) StartupWorkers() (string, error) {
	log.Infof("Using local workers")
	if s.workersCfg.Count < 0 {
		return "", fmt.Errorf("LocalWorkers must start with at least 1 worker (or zero for default #): %v", s.workersCfg.Count)
	} else if s.workersCfg.Count == 0 {
		s.workersCfg.Count = DefaultWorkerCount
	}

	// A log level of 0 corresponds to Panic. Default behavior shouldn't be to surpress all log output
	// besides log.Panic, so we set a default of Info.
	if s.workersCfg.LogLevel <= 0 {
		s.workersCfg.LogLevel = DefaultWorkerLogLevel
	}

	log.Infof("Using %d local workers", s.workersCfg.Count)

	bin, err := s.builder.Worker()
	if err != nil {
		return "", err
	}

	for i := 0; i < s.workersCfg.Count; i++ {
		httpPort := s.nextPort
		s.nextPort++
		thriftPort := s.nextPort
		s.nextPort++
		if err := s.cmds.Start(bin,
			"-thrift_addr", "localhost:"+strconv.Itoa(thriftPort),
			"-http_addr", "localhost:"+strconv.Itoa(httpPort),
			"-log_level", s.workersCfg.LogLevel.String(),
			"-repo", s.workersCfg.RepoDir,
		); err != nil {
			return "", err
		}
		if err := WaitForPort(httpPort); err != nil {
			return "", err
		}
		if err := WaitForPort(thriftPort); err != nil {
			return "", err
		}
	}

	return "local.local", nil
}
