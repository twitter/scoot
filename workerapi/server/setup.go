package server

import (
	"log"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/runner/execer/execers"
	osexec "github.com/scootdev/scoot/runner/execer/os"
	localrunner "github.com/scootdev/scoot/runner/local"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

type servers struct {
	thrift thrift.TServer
	http   *endpoints.TwitterServer
}

func makeServers(thrift thrift.TServer, http *endpoints.TwitterServer) servers {
	return servers{thrift, http}
}

// Creates the default MagicBag and JsonSchema for this Server and
// returns them.  These can be modified before calling RunServer
// Magic Bag does not include:
// - thrift.TServerTransport
// - *endpoints.TwitterServer
// - *temp.TempDir
// these should be added by callers before invoking RunServer
func Defaults() (*ice.MagicBag, jsonconfig.Schema) {
	bag := ice.NewMagicBag()
	bag.PutMany(
		thrift.NewTTransportFactory,
		func() thrift.TProtocolFactory {
			return thrift.NewTBinaryProtocolFactoryDefault()
		},

		endpoints.MakeStatsReceiver,
		func() endpoints.StatScope { return "workerserver" },

		// Create Execer Func
		func() execer.Execer {
			return execers.MakeSimExecerInterceptor(execers.NewSimExecer(nil), osexec.NewExecer())
		},

		// Create Runner Func
		func(ex execer.Execer, tmpDir *temp.TempDir) runner.Runner {
			outputCreator, err := localrunner.NewOutputCreator(tmpDir)
			if err != nil {
				log.Fatal("Error creating OutputCreatorr: ", err)
			}
			return localrunner.NewSimpleRunner(ex, snapshots.MakeTempCheckouter(tmpDir), outputCreator)
		},
		NewHandler,
		MakeServer,
		makeServers,
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
