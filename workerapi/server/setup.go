package server

import (
	"log"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/os/temp"
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
func Defaults() (*ice.MagicBag, jsonconfig.Schema) {
	bag := ice.NewMagicBag()
	schema := jsonconfig.Schema(make(map[string]jsonconfig.Implementations))
	return bag, schema
}

// Starts the Server based on the MagicBag and config schema provided
// this method blocks until the server completes running or an
// exception occurs.
func RunServer(
	bag *ice.MagicBag,
	schema jsonconfig.Schema,
	config []byte,
	thriftAddr string,
	httpAddr string) {

	stat := endpoints.MakeStatsReceiver("").Precision(time.Millisecond)
	twServer := endpoints.NewTwitterServer(httpAddr, stat)
	go twServer.Serve()

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transportFactory := thrift.NewTTransportFactory()

	tempDir, err := temp.TempDirDefault()
	if err != nil {
		log.Fatal("error creating temp dir: ", err)
	}

	stats := stat.Scope("workerserver")
	outputCreator, err := localrunner.NewOutputCreator(tempDir)
	if err != nil {
		log.Fatal("Error creating OutputCreatorr: ", err)
	}

	ex := execers.MakeSimExecerInterceptor(execers.NewSimExecer(nil), osexec.NewExecer())
	run := localrunner.NewSimpleRunner(ex, snapshots.MakeTempCheckouter(tempDir), outputCreator)
	handler := NewHandler(stats, run)
	err = Serve(handler, thriftAddr, transportFactory, protocolFactory)
	if err != nil {
		log.Fatal("Error serving Worker Server: ", err)
	}
}
