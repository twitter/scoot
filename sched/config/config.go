package config

import (
	"encoding/json"
	"fmt"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/distributor"
	"github.com/scootdev/scoot/sched/queue"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/sched/worker"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/scootdev/scoot/scootapi/server"
)

// Config is the top-level configuration for the Scheduler. It defines how to create
// each of our (configurable) dependencies and then creates the top-level handler.
type Config struct {
	Queue   QueueConfig
	Cluster ClusterConfig
	SagaLog SagaLogConfig
	Workers WorkersConfig
}

// Create creates the thrift handler (or returns an error describing why it couldn't)
func (c *Config) Create() (scoot.CloudScoot, error) {
	q, err := c.Queue.Create()
	if err != nil {
		return nil, err
	}

	cl, err := c.Cluster.Create()
	if err != nil || cl == nil {
		return nil, err
	}

	sl, err := c.SagaLog.Create()
	if err != nil {
		return nil, err
	}

	wf, err := c.Workers.Create()
	if err != nil {
		return nil, err
	}

	sc := saga.MakeSagaCoordinator(sl)
	// Make the scheduler and handler
	dist, err := distributor.NewPoolDistributorFromCluster(cl)
	if err != nil {
		return nil, err
	}
	schedImpl := scheduler.NewScheduler(dist, sc, wf)
	handler := server.NewHandler(q, sc)

	// Go Routine which takes data from work queue and schedules it
	go func() {
		scheduler.GenerateWork(schedImpl, q.Chan())
	}()

	return handler, nil
}

type QueueConfig interface {
	Create() (queue.Queue, error)
}

type ClusterConfig interface {
	Create() (cluster.Cluster, error)
}

type SagaLogConfig interface {
	Create() (saga.SagaLog, error)
}

type WorkersConfig interface {
	Create() (worker.WorkerFactory, error)
}

// Scheduler config parsed from JSON. Each should parse into an empty string or a JSON object that has a "type" field which we will use to pick which kind of config to parse it ias.
type topLevelConfig struct {
	Cluster json.RawMessage
	Queue   json.RawMessage
	SagaLog json.RawMessage
	Workers json.RawMessage
}

type typeConfig struct {
	Type string
}

var emptyJson = []byte("{}")

func parseType(data json.RawMessage) string {
	if len(data) == 0 {
		return ""
	}

	var t typeConfig
	err := json.Unmarshal(data, &t)
	if err != nil {
		return ""
	}
	return t.Type
}

// We'd like to take the data, the domain, and a map[string]interface{}
// But Parse struct doesn't have a map[string]interface{}, it has a map[string]FooConfig (for different values of Foo)
// So have the caller call parseType, then look it up in the map, and pass it to us.
func parseDomain(data json.RawMessage, domain string, config interface{}) (interface{}, error) {
	t := parseType(data)
	if config == nil {
		return nil, fmt.Errorf("No parser for type %s in domain %s", t, domain)
	}
	if len(data) == 0 {
		data = emptyJson
	}
	err := json.Unmarshal(data, config)
	if err != nil {
		return nil, fmt.Errorf("Couldn't parse %s: %v (config: %s; type: %s)", domain, err, data, t)
	}
	return config, nil
}

// Parser holds how to parse our configs. For each configurable dependency, it holds
// options for how to parse it. It will look at the "type" field in the config and
// look that up in the map. (If the object is not present in the JSON, it will lookup
// the empty string. If you want to set a default, set Parser.Foo[""] = &FooBarConfig{Type: "bar", Setting: Value})
type Parser struct {
	Queue   map[string]QueueConfig
	Cluster map[string]ClusterConfig
	SagaLog map[string]SagaLogConfig
	Workers map[string]WorkersConfig
}

// Create parses and creates in one step.
func (p *Parser) Create(configText []byte) (scoot.CloudScoot, error) {
	c, err := p.Parse(configText)
	if err != nil {
		return nil, err
	}
	return c.Create()
}

// Generates the JSON config that results from the empty string; useful for showing a complete configuration.
func (p *Parser) DefaultJSON() ([]byte, error) {
	i, err := p.Parse(nil)
	if err != nil {
		return nil, err
	}
	return json.Marshal(i)
}

func (p *Parser) Parse(configText []byte) (*Config, error) {
	// TODO(dbentley): allow this to refer to a file instead of being a giant string
	if len(configText) == 0 {
		configText = emptyJson
	}
	var cfg topLevelConfig
	err := json.Unmarshal(configText, &cfg)
	if err != nil {
		return nil, fmt.Errorf("Couldn't Parse top-level config: %v", err)
	}

	r := &Config{}

	parsed, err := parseDomain(cfg.Queue, "Queue", p.Queue[parseType(cfg.Queue)])
	if err != nil {
		return nil, err
	}
	queueConfig, ok := parsed.(QueueConfig)
	if !ok {
		return nil, fmt.Errorf("Didn't get a QueueConfig: %v %T", parsed, parsed)
	}
	r.Queue = queueConfig

	parsed, err = parseDomain(cfg.Cluster, "Cluster", p.Cluster[parseType(cfg.Cluster)])
	if err != nil {
		return nil, err
	}
	clusterConfig, ok := parsed.(ClusterConfig)
	if !ok {
		return nil, fmt.Errorf("Didn't get a ClusterConfig: %v %T", parsed, parsed)
	}
	r.Cluster = clusterConfig

	parsed, err = parseDomain(cfg.SagaLog, "SagaLog", p.SagaLog[parseType(cfg.SagaLog)])
	if err != nil {
		return nil, err
	}
	sagalogConfig, ok := parsed.(SagaLogConfig)
	if !ok {
		return nil, fmt.Errorf("Didn't get a SagaLogConfig: %v %T", parsed, parsed)
	}
	r.SagaLog = sagalogConfig

	parsed, err = parseDomain(cfg.Workers, "Workers", p.Workers[parseType(cfg.Workers)])
	if err != nil {
		return nil, err
	}
	workersConfig, ok := parsed.(WorkersConfig)
	if !ok {
		return nil, fmt.Errorf("Didn't get a WorkersConfig: %v %T", parsed, parsed)
	}
	r.Workers = workersConfig

	return r, nil
}
