package server

import (
	"log"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/runner/execer/execers"
	osexec "github.com/scootdev/scoot/runner/execer/os"
	localrunner "github.com/scootdev/scoot/runner/local"
	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/snapshots"
	"github.com/scootdev/scoot/workerapi/gen-go/worker"
)

type servers struct {
	thrift thrift.TServer
	http   *endpoints.TwitterServer
}

func makeServers(thrift thrift.TServer, http *endpoints.TwitterServer) servers {
	return servers{thrift, http}
}

// Creates the default MagicBag and JsonSchema for this Server and
// returns them.  These functions can be overridden before calling RunServer
func Defaults() (*ice.MagicBag, jsonconfig.Schema) {
	bag := ice.NewMagicBag()
	bag.PutMany(

		func() (thrift.TServerTransport, error) { return thrift.NewTServerSocket("localhost:2000") },

		func() thrift.TTransportFactory { return thrift.NewTTransportFactory() },

		func() thrift.TProtocolFactory {
			return thrift.NewTBinaryProtocolFactoryDefault()
		},

		func() endpoints.StatScope { return "workerserver" },

		func(scope endpoints.StatScope) stats.StatsReceiver { return endpoints.MakeStatsReceiver(scope) },

		func(s stats.StatsReceiver) *endpoints.TwitterServer {
			return endpoints.NewTwitterServer("localhost:2001", s)
		},

		func() execer.Execer {
			return execers.MakeSimExecerInterceptor(execers.NewSimExecer(), osexec.NewExecer())
		},

		func() (*temp.TempDir, error) { return temp.TempDirDefault() },

		func(tmpDir *temp.TempDir) snapshot.Filer {
			return snapshots.MakeTempCheckouterFiler(tmpDir)
		},

		func(tmpDir *temp.TempDir) (runner.OutputCreator, error) {
			return localrunner.NewOutputCreator(tmpDir)
		},

		func(
			ex execer.Execer,
			outputCreator runner.OutputCreator,
			filer snapshot.Filer) runner.Runner {
			return localrunner.NewSimpleRunner(ex, filer, outputCreator)
		},

		func(stat stats.StatsReceiver, r runner.Runner) worker.Worker {
			return NewHandler(stat, r)
		},

		func(
			handler worker.Worker,
			transport thrift.TServerTransport,
			transportFactory thrift.TTransportFactory,
			protocolFactory thrift.TProtocolFactory) thrift.TServer {
			return MakeServer(handler, transport, transportFactory, protocolFactory)
		},

		func(thrift thrift.TServer, http *endpoints.TwitterServer) servers {
			return makeServers(thrift, http)
		},
	)

	schema := jsonconfig.Schema(make(map[string]jsonconfig.Implementations))
	return bag, schema
}

// Starts the Server based on the MagicBag and config schema provided
// this method blocks until the server completes running or an
// exception occurs.
func RunServer(
	bag *ice.MagicBag,
	schema jsonconfig.Schema,
	config []byte) {

	// Parse Config
	mod, err := schema.Parse(config)
	if err != nil {
		log.Fatal("Error configuring Worker: ", err)
	}

	// Initialize Objects Based on Config Settings
	bag.InstallModule(mod)

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
