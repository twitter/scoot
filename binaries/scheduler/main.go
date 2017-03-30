package main

//go:generate go-bindata -pkg "config" -o ./config/config.go config

import (
	"flag"
	log "github.com/inconshreveable/log15"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/scootdev/scoot/binaries/scheduler/config"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/scootapi"
	"github.com/scootdev/scoot/scootapi/server"
)

// Set Flags Needed by this Server
//TODO: add support for in-memory workers doing real work with gitdb.
var thriftAddr = flag.String("thrift_addr", scootapi.DefaultSched_Thrift, "Bind address for api server.")
var httpAddr = flag.String("http_addr", scootapi.DefaultSched_HTTP, "addr to serve http on")
var configFlag = flag.String("config", "local.memory", "Scheduler Config (either a filename like local.memory or JSON text")

func main() {
	flag.Parse()

	// log.SetFlags(log.LstdFlags | log.LUTC | log.Lshortfile)

	configText, err := jsonconfig.GetConfigText(*configFlag, config.Asset)
	if err != nil {
		log.Crit(err.Error())
	}
	bag, schema := server.Defaults()
	bag.PutMany(
		func() (thrift.TServerTransport, error) { return thrift.NewTServerSocket(*thriftAddr) },

		func(s stats.StatsReceiver) *endpoints.TwitterServer {
			return endpoints.NewTwitterServer(endpoints.Addr(*httpAddr), s, nil)
		},

		func() (*temp.TempDir, error) {
			return temp.NewTempDir("", "sched")
		},
	)

	log.Debug("Starting Cloud Scoot API Server & Scheduler on", *thriftAddr)
	server.RunServer(bag, schema, configText)
}
