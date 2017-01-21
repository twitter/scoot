package frontend

import (
	"log"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/ice"
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

// Creates an MagicBag and a JsconSchema for this server and returns them
func Defaults() (*ice.MagicBag, jsonconfig.Schema) {
	bag := ice.NewMagicBag()
	bag.PutMany(
		func() (thrift.TServerTransport, error) { return thrift.NewTServerSocket("localhost:9088") },

		func() thrift.TTransportFactory { return thrift.NewTTransportFactory() },

		func() thrift.TProtocolFactory { return thrift.NewTBinaryProtocolFactoryDefault() },

		func() endpoints.StatScope { return "apiserver" },

		func(scope endpoints.StatScope) stats.StatsReceiver {
			return endpoints.MakeStatsReceiver(scope).Precision(time.Millisecond)
		},

		func(
			c *scootapi.CloudScootClient,
			stat stats.StatsReceiver) scoot.CloudScoot {
			return NewHandler(c, stat)
		},

		func(
			h scoot.CloudScoot,
			t thrift.TServerTransport,
			tf thrift.TTransportFactory,
			pf thrift.TProtocolFactory) thrift.TServer {
			return MakeServer(h, t, tf, pf)
		},

		func(s stats.StatsReceiver) *endpoints.TwitterServer {
			return endpoints.NewTwitterServer("localhost:9089", s, nil)
		},

		func(t thrift.TServer, h *endpoints.TwitterServer) servers {
			return makeServers(t, h)
		},
	)

	schema := jsonconfig.Schema(map[string]jsonconfig.Implementations{})

	return bag, schema
}

// Starts the Server based on the MagicBag and config schema provided
// this method blocks until the server completes running or an error occurs.
func RunServer(bag *ice.MagicBag, schema jsonconfig.Schema, config []byte) {
	// Parse Config
	mod, err := schema.Parse(config)
	if err != nil {
		log.Fatal("Error configuring Scoot APIServer: ", err)
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
	if servers.thrift != nil {
		go func() {
			errCh <- servers.thrift.Serve()
		}()
	}
	log.Fatal("Error serving: ", <-errCh)
}
