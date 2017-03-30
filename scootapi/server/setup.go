package server

import (
	"github.com/scootdev/scoot/common/log"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/config/scootconfig"
	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/scootapi"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
)

type servers struct {
	thrift thrift.TServer
	http   *endpoints.TwitterServer
}

func makeServers(
	thrift thrift.TServer,
	http *endpoints.TwitterServer) servers {
	return servers{thrift, http}
}

// Creates an MagicBag and a JsonSchema for this server and returns them
func Defaults() (*ice.MagicBag, jsonconfig.Schema) {
	bag := ice.NewMagicBag()
	bag.PutMany(
		func() (thrift.TServerTransport, error) { return thrift.NewTServerSocket(scootapi.DefaultSched_Thrift) },

		func() thrift.TTransportFactory { return thrift.NewTTransportFactory() },

		func() endpoints.StatScope { return "scheduler" },

		func(scope endpoints.StatScope) stats.StatsReceiver {
			return endpoints.MakeStatsReceiver(scope).Precision(time.Millisecond)
		},

		func(
			cl *cluster.Cluster,
			sc saga.SagaCoordinator,
			rf func(cluster.Node) runner.Service,
			config scheduler.SchedulerConfig,
			stat stats.StatsReceiver) scheduler.Scheduler {
			return scheduler.NewStatefulSchedulerFromCluster(cl, sc, rf, config, stat)
		},

		func(
			s scheduler.Scheduler,
			sc saga.SagaCoordinator,
			stat stats.StatsReceiver) scoot.CloudScoot {
			return NewHandler(s, sc, stat)
		},

		func(
			h scoot.CloudScoot,
			t thrift.TServerTransport,
			tf thrift.TTransportFactory,
			pf thrift.TProtocolFactory) thrift.TServer {
			return MakeServer(h, t, tf, pf)
		},

		func(s stats.StatsReceiver) *endpoints.TwitterServer {
			return endpoints.NewTwitterServer(endpoints.Addr(scootapi.DefaultSched_HTTP), s, nil)
		},

		func(t thrift.TServer, h *endpoints.TwitterServer) servers {
			return makeServers(t, h)
		},

		func(log saga.SagaLog) saga.SagaCoordinator {
			return saga.MakeSagaCoordinator(log)
		},

		func() thrift.TProtocolFactory {
			return thrift.NewTBinaryProtocolFactoryDefault()
		},
	)

	schema := jsonconfig.Schema(map[string]jsonconfig.Implementations{
		"SagaLog": {
			"memory": &scootconfig.InMemorySagaLogConfig{},
			"file":   &scootconfig.FileSagaLogConfig{},
			"":       &scootconfig.InMemorySagaLogConfig{},
		},
		"Cluster": {
			"memory": &scootconfig.ClusterMemoryConfig{},
			"local":  &scootconfig.ClusterLocalConfig{},
			"": &scootconfig.ClusterMemoryConfig{
				Type:  "memory",
				Count: 10,
			},
		},
		"Workers": {
			"local": &scootconfig.WorkersLocalConfig{},
			"rpc":   &scootconfig.WorkersThriftConfig{},
			"":      &scootconfig.WorkersLocalConfig{Type: "local"},
		},
		"SchedulerConfig": {
			"stateful": &scootconfig.StatefulSchedulerConfig{},
			"": &scootconfig.StatefulSchedulerConfig{
				Type:                 "stateful",
				MaxRetriesPerTask:    0,
				DefaultTaskTimeoutMs: 30 * 60 * 1000, // 30m
				RunnerOverheadMs:     10 * 60 * 1000, // 10m
			},
		},
	})

	return bag, schema
}

// Starts the Server based on the MagicBag and config schema provided
// this method blocks until the server completes running or an error occurs.
func RunServer(bag *ice.MagicBag, schema jsonconfig.Schema, config []byte) {
	// Parse Config
	log.Info("scootapi/server RunServer(), config is:", string(config))
	mod, err := schema.Parse(config)
	if err != nil {
		log.Crit("Error configuring Scoot API: ", err)
	}

	// Initialize Objects Based on Config Settings
	bag.InstallModule(mod)

	// Run Servers
	var servers servers
	err = bag.Extract(&servers)
	if err != nil {
		log.Crit("Error injecting servers", err)
	}

	errCh := make(chan error)
	go func() {
		errCh <- servers.http.Serve()
	}()
	go func() {
		errCh <- servers.thrift.Serve()
	}()
	log.Crit("Error serving: ", <-errCh)
}
