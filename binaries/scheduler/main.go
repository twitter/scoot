package main

//go:generate go-bindata -pkg "config" -o ./config/config.go config

import (
	"flag"
	"log"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/scootdev/scoot/binaries/scheduler/config"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/scootapi/server"
	"github.com/scootdev/scoot/scootapi/setup"
	"github.com/scootdev/scoot/snapshot"
)

// Set Flags Needed by this Server
var thriftAddr = flag.String("thrift_addr", "localhost:9090", "Bind address for api server.")
var httpAddr = flag.String("http_addr", "localhost:9091", "port to serve http on")
var configFlag = flag.String("config", "local.memory", "Scheduler Config (either a filename like local.memory or JSON text")
var repoDir = flag.String("repo", "", "In-memory worker: abs dir path to a git repo to run against (don't use important repos yet!).")
var storeHandle = flag.String("bundlestore", "", "In-memory worker: abs file path or http URL where repo uploads/downloads bundles.")

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
			return endpoints.NewTwitterServer(*httpAddr, s, nil)
		},

		func(tmpDir *temp.TempDir) (snapshot.Filer, error) {
			// Only invoked if we're using in-memory workers.
			if db, err := setup.NewGitDB(tmpDir, *repoDir, *storeHandle); err != nil {
				return nil, err
			} else {
				return snapshot.NewDBAdapter(db), nil
			}
		},
	)

	log.Println("Starting Cloud Scoot API Server & Scheduler on", *thriftAddr)
	server.RunServer(bag, schema, configText)
}
