package server

import (
	"log"

	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/ice"

	// For putting into ice.MagicBag
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/sched/scheduler"

	// For putting into jsonconfig.Options
	"github.com/scootdev/scoot/config/scootconfig"
	"github.com/scootdev/scoot/saga/sagalogs"
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

// Creates an MagicBag and a JsconSchema for this server and returns them
func Defaults() (*ice.MagicBag, jsonconfig.Schema) {
	bag := ice.NewMagicBag()
	bag.PutMany(
		endpoints.MakeStatsReceiver,
		MakeServer,
		NewHandler,
		makeServers,
		saga.MakeSagaCoordinator,
		sagalogs.MakeInMemorySagaLog,
		scheduler.NewStatefulSchedulerFromCluster,
		thrift.NewTTransportFactory,
		func() thrift.TProtocolFactory {
			return thrift.NewTBinaryProtocolFactoryDefault()
		},
	)

	schema := jsonconfig.Schema(map[string]jsonconfig.Implementations{
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
			"":         &scootconfig.StatefulSchedulerConfig{Type: "stateful", MaxRetriesPerTask: 0},
		},
	})

	return bag, schema
}

// Starts the Server based on the MagicBag and config schema provided
// this method blocks until the server completes running or an error occurs.
func RunServer(bag *ice.MagicBag, schema jsonconfig.Schema, config []byte) {
	// Parse Config
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
	log.Fatal("Error serving: ", <-errCh)
}
