package setup

import (
	"fmt"
	"log"
	"strconv"
)

// WorkersStrategy is a strategy to start workers and returns the config to pass to a scheduler to talk to them
type WorkersStrategy interface {
	// StartupWorkers should startup necessary workers, and returns the config to pass to Scheduler, or an error
	StartupWorkers() (string, error)
}

// InMemoryWorkersStrategy will use in-memory workers (to test the Scheduler logic)
type InMemoryWorkersStrategy struct {
	workersFlag string
}

// NewInMemoryWorkers creates a new InMemoryWorkersStartup
func NewInMemoryWorkers(workersFlag string) *InMemoryWorkersStrategy {
	return &InMemoryWorkersStrategy{workersFlag: workersFlag}
}

func (s *InMemoryWorkersStrategy) StartupWorkers() (string, error) {
	log.Println("Using in-memory workers")
	if s.workersFlag == "" {
		return "local.memory", nil
	}

	return fmt.Sprintf(`{"Cluster": {"Type": "memory", "Count": %s}, "Workers": {"Type": "local"}}`, s.workersFlag), nil
}

// LocalWorkersStrategy will startup workers running locally
type LocalWorkersStrategy struct {
	workersFlag string
	builder     Builder
	cmds        *Cmds
	nextPort    int
}

// Start workers at 10100 for clarity
const firstWorkerPort = 10100

// NewLocalWorkers creates a new LocalWorkersStartup
func NewLocalWorkers(workersFlag string, builder Builder, cmds *Cmds) *LocalWorkersStrategy {
	return &LocalWorkersStrategy{
		workersFlag: workersFlag,
		builder:     builder,
		cmds:        cmds,
		nextPort:    firstWorkerPort,
	}
}

func (s *LocalWorkersStrategy) StartupWorkers() (string, error) {
	log.Printf("Using local workers")
	numWorkers, err := getNumWorkers(s.workersFlag)
	if err != nil {
		return "", err
	}

	if numWorkers < 1 {
		return "", fmt.Errorf("LocalWorkers must start with at least 1 worker: %v", numWorkers)
	}

	log.Printf("Using %d local workers", numWorkers)

	bin, err := s.builder.Worker()
	if err != nil {
		return "", err
	}

	for i := 0; i < numWorkers; i++ {
		httpPort := strconv.Itoa(s.nextPort)
		s.nextPort++
		thriftPort := strconv.Itoa(s.nextPort)
		s.nextPort++
		if err := s.cmds.Start(bin, "-thrift_addr", "localhost:"+thriftPort, "-http_addr", "localhost:"+httpPort); err != nil {
			return "", err
		}
		if err := WaitForPort(httpPort); err != nil {
			return "", err
		}
	}

	return "local.local", nil
}

// parses a number of workers from the flag, with a reasonable default
func getNumWorkers(workersFlag string) (int, error) {
	if workersFlag == "" {
		return 5, nil
	}
	return strconv.Atoi(workersFlag)
}
