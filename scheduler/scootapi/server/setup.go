package server

import (
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/execution"
	"github.com/twitter/scoot/common/cloud/cluster"
	"github.com/twitter/scoot/common/config/jsonconfig"
	"github.com/twitter/scoot/common/config/scootconfig"
	"github.com/twitter/scoot/common/endpoints"
	"github.com/twitter/scoot/common/ice"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/scheduler/saga"
	"github.com/twitter/scoot/scheduler/sched/scheduler"
	"github.com/twitter/scoot/scheduler/scootapi/client"
	"github.com/twitter/scoot/scheduler/scootapi/gen-go/scoot"
	"github.com/twitter/scoot/workerserver/runner"
)

type servers struct {
	thrift thrift.TServer
	http   *endpoints.TwitterServer
	grpc   bazel.GRPCServer
}

func makeServers(
	thrift thrift.TServer,
	http *endpoints.TwitterServer,
	grpc bazel.GRPCServer) servers {
	return servers{thrift, http, grpc}
}

// Creates an MagicBag and a JsonSchema for this server and returns them
func Defaults() (*ice.MagicBag, jsonconfig.Schema) {
	bag := ice.NewMagicBag()
	bag.PutMany(
		func() (thrift.TServerTransport, error) { return thrift.NewTServerSocket(client.DefaultSched_Thrift) },

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
			return endpoints.NewTwitterServer(endpoints.Addr(client.DefaultSched_HTTP), s, nil)
		},

		func(t thrift.TServer, h *endpoints.TwitterServer, g bazel.GRPCServer) servers {
			return makeServers(t, h, g)
		},

		func(log saga.SagaLog) saga.SagaCoordinator {
			return saga.MakeSagaCoordinator(log)
		},

		func() thrift.TProtocolFactory {
			return thrift.NewTBinaryProtocolFactoryDefault()
		},

		func() *bazel.GRPCConfig {
			return &bazel.GRPCConfig{
				GRPCAddr: client.DefaultSched_GRPC,
			}
		},

		func(gc *bazel.GRPCConfig, s scheduler.Scheduler, stat stats.StatsReceiver) bazel.GRPCServer {
			return execution.MakeExecutionServer(gc, s, stat)
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
				Type:               "stateful",
				MaxRetriesPerTask:  0,
				DefaultTaskTimeout: "30m",
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
		log.Fatal("Error configuring Scoot API: ", err)
	}

	// Initialize Objects Based on Config Settings
	bag.InstallModule(mod)

	// Run Servers
	var servers servers
	err = bag.Extract(&servers)
	if err != nil {
		log.Fatal("Error injecting servers", err)
	}

	errCh := make(chan error)
	go func() {
		errCh <- servers.http.Serve()
	}()
	go func() {
		errCh <- servers.thrift.Serve()
	}()
	go func() {
		errCh <- servers.grpc.Serve()
	}()
	log.Fatal("Error serving: ", <-errCh)
}
