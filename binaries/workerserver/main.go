package main

//go:generate go-bindata -pkg "config" -o ./config/config.go config

import (
	"flag"
	"log"
	"net/http"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/binaries/workerserver/config"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/scootapi"
	"github.com/scootdev/scoot/scootapi/setup"
	"github.com/scootdev/scoot/snapshot"

	"github.com/scootdev/scoot/workerapi/server"
)

var thriftAddr = flag.String("thrift_addr", scootapi.DefaultWorker_Thrift, "addr to serve thrift on")
var httpAddr = flag.String("http_addr", scootapi.DefaultWorker_HTTP, "addr to serve http on")
var configFlag = flag.String("config", "local.local", "Worker Server Config (either a filename like local.local or JSON text")
var memCapFlag = flag.Uint64("mem_cap", 0, "Kill runs that exceed this amount of memory, in bytes. Zero means no limit.")
var repoDir = flag.String("repo", "", "Abs dir path to a git repo to run against (don't use important repos yet!).")
var storeHandle = flag.String("bundlestore", "", "Abs file path or http URL where repo uploads/downloads bundles.")

func main() {
	flag.Parse()

	configText, err := jsonconfig.GetConfigText(*configFlag, config.Asset)
	if err != nil {
		log.Fatal(err)
	}

	bag, schema := server.Defaults()
	bag.PutMany(
		func() (thrift.TServerTransport, error) { return thrift.NewTServerSocket(*thriftAddr) },

		func(s stats.StatsReceiver, handlers map[string]http.Handler) *endpoints.TwitterServer {
			return endpoints.NewTwitterServer(endpoints.Addr(*httpAddr), s, handlers)
		},

		func(tmpDir *temp.TempDir) (snapshot.Filer, error) {
			if db, err := setup.NewGitDB(tmpDir, *repoDir, *storeHandle); err != nil {
				return nil, err
			} else {
				return snapshot.NewDBAdapter(db), nil
			}
		},

		func() server.WorkerUri {
			return server.WorkerUri("http://" + *httpAddr)
		},

		func() execer.Memory {
			return execer.Memory(*memCapFlag)
		},
	)

	log.Println("Serving thrift on", *thriftAddr)
	server.RunServer(bag, schema, configText)
}
