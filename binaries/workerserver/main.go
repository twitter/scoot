package main

//go:generate go-bindata -pkg "config" -o ./config/config.go config

import (
	"flag"
	"log"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/binaries/workerserver/config"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/snapshots"
	"github.com/scootdev/scoot/workerapi/server"
)

var thriftAddr = flag.String("thrift_addr", "localhost:9090", "port to serve thrift on")
var httpAddr = flag.String("http_addr", "localhost:9091", "port to serve http on")
var configFlag = flag.String("config", "local.local", "Worker Server Config (either a filename like local.local or JSON text")

func main() {
	flag.Parse()

	configText, err := jsonconfig.GetConfigText(*configFlag, config.Asset)
	if err != nil {
		log.Fatal(err)
	}

	bag, schema := server.Defaults()
	bag.PutMany(
		func() (thrift.TServerTransport, error) { return thrift.NewTServerSocket(*thriftAddr) },
		func(s stats.StatsReceiver) *endpoints.TwitterServer {
			return endpoints.NewTwitterServer(*httpAddr, s)
		},
		func() (*temp.TempDir, error) { return temp.TempDirDefault() },
		func(tmpDir *temp.TempDir) snapshot.Checkouter {
			return snapshots.MakeTempCheckouter(tmpDir)
		},
	)

	server.RunServer(bag, schema, configText)
}
