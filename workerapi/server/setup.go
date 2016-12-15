package server

import (
	"log"
	"net/http"
	"strings"
	"time"

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
	"github.com/scootdev/scoot/workerapi/gen-go/worker"
)

type servers struct {
	thrift thrift.TServer
	http   *endpoints.TwitterServer
}

type WorkerUri string

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

		func(scope endpoints.StatScope) stats.StatsReceiver {
			return endpoints.MakeStatsReceiver(scope).Precision(time.Millisecond)
		},

		func(outputCreator localrunner.HttpOutputCreator) map[string]http.Handler {
			return map[string]http.Handler{outputCreator.HttpPath(): outputCreator}
		},

		func(s stats.StatsReceiver, handlers map[string]http.Handler) *endpoints.TwitterServer {
			return endpoints.NewTwitterServer("localhost:2001", s, handlers)
		},

		func() execer.Execer {
			return execers.MakeSimExecerInterceptor(execers.NewSimExecer(), osexec.NewExecer())
		},

		func() (*temp.TempDir, error) { return temp.TempDirDefault() },

		func(tmpDir *temp.TempDir, uri WorkerUri) (localrunner.HttpOutputCreator, error) {
			outDir, err := tmpDir.FixedDir("output")
			if err != nil {
				return nil, err
			}
			return localrunner.NewHttpOutputCreator(outDir, strings.TrimSuffix(string(uri), "/")+"/output")
		},

		func(
			ex execer.Execer,
			outputCreator localrunner.HttpOutputCreator,
			filer snapshot.Filer) runner.Runner {
			return localrunner.NewSimpleRunner(ex, filer, outputCreator)
		},

		func() WorkerUri {
			return "http://localhost:2001"
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
