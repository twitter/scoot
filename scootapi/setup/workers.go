package setup

import (
	"fmt"
	log "github.com/scootdev/scoot/common/logger"
	"strconv"

	"github.com/scootdev/scoot/scootapi"
)

const DefaultWorkerCount int = 5

// WorkersStrategy is a strategy to start workers and returns the config to pass to a scheduler to talk to them
type WorkersStrategy interface {
	// StartupWorkers should startup necessary workers, and returns the config to pass to Scheduler, or an error
	StartupWorkers() (string, error)
}

// In addition to count, we'll optionally want repoDir to initialize workers' gitdb.
// Whatever is unset will be given a default value.
type WorkerConfig struct {
	Count   int
	RepoDir string
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
		return "local.memory", nil
	}

	return fmt.Sprintf(`{"Cluster": {"Type": "memory", "Count": %d}, "Workers": {"Type": "local"}}`, s.workersCfg.Count), nil
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
	log.Info("Using local workers")
	if s.workersCfg.Count < 0 {
		return "", fmt.Errorf("LocalWorkers must start with at least 1 worker (or zero for default #): %v", s.workersCfg.Count)
	} else if s.workersCfg.Count == 0 {
		s.workersCfg.Count = DefaultWorkerCount
	}

	log.Info("Using %d local workers", s.workersCfg.Count)

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
