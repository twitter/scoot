package config

import (
	"encoding/json"
	"fmt"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/common/stats"
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
	Report  ReportConfig
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

	stat, err := c.Report.Create()
	if err != nil {
		return nil, err
	}

	sc := saga.MakeSagaCoordinator(sl)
	// Make the scheduler and handler
	dist, err := distributor.NewPoolDistributorFromCluster(cl)
	if err != nil {
		return nil, err
	}
	schedImpl := scheduler.NewScheduler(dist, sc, wf, stat.Scope("scheduler"))
	handler := server.NewHandler(q, sc, stat.Scope("handler"))

	// Go Routine which takes data from work queue and schedules it
	go func() {
		scheduler.GenerateWork(schedImpl, q.Chan(), stat.Scope("generator"))
	}()

	return handler, nil
}

type QueueConfig interface {
	Create() (queue.Queue, error)
}

type ClusterConfig interface {
	Create() (*cluster.Cluster, error)
}

type SagaLogConfig interface {
	Create() (saga.SagaLog, error)
}

type WorkersConfig interface {
	Create() (worker.WorkerFactory, error)
}

type ReportConfig interface {
	Create() (stats.StatsReceiver, error)
}

// Scheduler config parsed from JSON. Each should parse into an empty string or a JSON object that has a "type" field which we will use to pick which kind of config to parse it ias.
type topLevelConfig struct {
	Cluster json.RawMessage
	Queue   json.RawMessage
	SagaLog json.RawMessage
	Workers json.RawMessage
	Report  json.RawMessage
}

type typeConfig struct {
	Type string
}

var emptyJson = []byte("{}")

func parseType(data json.RawMessage) (string, []byte) {
	if len(data) == 0 {
		return "", emptyJson
	}

	var t typeConfig
	err := json.Unmarshal(data, &t)
	if err != nil {
		return "", emptyJson
	}
	return t.Type, data
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
	Report  map[string]ReportConfig
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

	// Now we parse each config. To do this, we:
	// 1) Parse its type
	// 2) Find the FooConfig for this type
	// 3) Unmarshal into the FooConfig
	// 4) Set this config in the result

	queueType, queueData := parseType(cfg.Queue)
	queueConfig, ok := p.Queue[queueType]
	if !ok {
		return nil, fmt.Errorf("No parser for queue type %s", queueType)
	}
	err = json.Unmarshal(queueData, &queueConfig)
	if err != nil {
		return nil, fmt.Errorf("Couldn't parse Queue: %v (config: %s; type: %s)", err, queueData, queueType)
	}
	r.Queue = queueConfig

	clusterType, clusterData := parseType(cfg.Cluster)
	clusterConfig, ok := p.Cluster[clusterType]
	if !ok {
		return nil, fmt.Errorf("No parser for cluster type %s", clusterType)
	}
	err = json.Unmarshal(clusterData, &clusterConfig)
	if err != nil {
		return nil, fmt.Errorf("Couldn't parse Cluster: %v (config: %s; type: %s)", err, clusterData, clusterType)
	}
	r.Cluster = clusterConfig

	logType, logData := parseType(cfg.SagaLog)
	logConfig, ok := p.SagaLog[logType]
	if !ok {
		return nil, fmt.Errorf("No parser for log type %s", logType)
	}
	err = json.Unmarshal(logData, &logConfig)
	if err != nil {
		return nil, fmt.Errorf("Couldn't parse Log: %v (config: %s; type: %s)", err, logData, logType)
	}
	r.SagaLog = logConfig

	workersType, workersData := parseType(cfg.Workers)
	workersConfig, ok := p.Workers[workersType]
	if !ok {
		return nil, fmt.Errorf("No parser for workers type %s", workersType)
	}
	err = json.Unmarshal(workersData, &workersConfig)
	if err != nil {
		return nil, fmt.Errorf("Couldn't parse Workers: %v (config: %s; type: %s)", err, workersData, workersType)
	}
	r.Workers = workersConfig

	reportType, reportData := parseType(cfg.Report)
	reportConfig, ok := p.Report[reportType]
	if !ok {
		return nil, fmt.Errorf("No parser for report type %s", reportType)
	}
	err = json.Unmarshal(reportData, &reportConfig)
	if err != nil {
		return nil, fmt.Errorf("Couldn't parse Report: %v (config: %s; type: %s)", err, reportData, reportType)
	}
	r.Report = reportConfig

	return r, nil
}
