package main

import (
	"flag"
	"log"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/sched/config"
	"github.com/scootdev/scoot/scootapi/server"
	"golang.org/x/net/context"
)

var addr = flag.String("addr", "localhost:9090", "Bind address for api server.")
var cfgFile = flag.String("cfg", "", "Path to sched overlay config file.")

func main() {
	log.Println("Starting Cloud Scoot API Server & Scheduler")
	flag.Parse()

	plugins := config.Plugins{}
	ctx := context.Background()
	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	// Construct scootapi server handler based on config.
	cfg, err := config.DefaultConfig(*cfgFile, plugins)
	if err != nil {
		log.Fatal("Error configuring Scoot API: ", err)
	}

	handler, err := config.ConfigureCloudScoot(cfg, ctx, transportFactory, protocolFactory)
	if err != nil {
		log.Fatal("Error configuring Scoot API: ", err)
	}

	// Start API Server
	log.Println("Starting API Server")
	err = server.Serve(handler, *addr, transportFactory, protocolFactory)
	if err != nil {
		log.Fatal("Error serving Scoot API: ", err)
	}
}
