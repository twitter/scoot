package main

//go:generate go-bindata -pkg "config" -o ./config/config.go config

import (
	"flag"
	"fmt"
	"log"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/binaries/scheduler/config"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/scootapi/server"
)

// Set Flags Needed by this Server
var thriftAddr = flag.String("thrift_addr", "localhost:9090", "Bind address for api server.")
var httpAddr = flag.String("http_addr", "localhost:9091", "port to serve http on")

var configFileName = flag.String("config", "local.json", "Scheduler Config File")

func main() {

	flag.Parse()
	log.Printf("Scheduler: reading config %+v", *configFileName)
	config, err := config.Asset(fmt.Sprintf("config/%v", *configFileName))

	if err != nil {
		log.Fatalf("Error Loading Config File: %v, with Error: %v", configFileName, err)
	}

	bag, schema := server.Defaults()
	bag.PutMany(
		func() endpoints.StatScope { return "scheduler" },
		func() (thrift.TServerTransport, error) { return thrift.NewTServerSocket(*thriftAddr) },
		func(s stats.StatsReceiver) *endpoints.TwitterServer {
			return endpoints.NewTwitterServer(*httpAddr, s)
		},
	)

	log.Println("Starting Cloud Scoot API Server & Scheduler on", *thriftAddr)
	server.RunServer(bag, schema, []byte(config))
}
