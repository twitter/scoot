package main

import (
	"flag"
	"log"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/cloud/cluster/local"
	"github.com/scootdev/scoot/sched/config"
	"github.com/scootdev/scoot/scootapi/server"
)

var addr = flag.String("addr", "localhost:9090", "Bind address for api server.")
var cfgText = flag.String("sched_config", "", "Scheduler Configuration.")

func main() {
	log.Println("Starting Cloud Scoot API Server & Scheduler")
	flag.Parse()

	parser := config.DefaultParser()
	parser.Workers[""] = &config.RPCWorkersConfig{Type: "rpc"}
	parser.Cluster[""] = &local.ClusterLocalConfig{Type: "local"}
	parser.Cluster["local"] = &local.ClusterLocalConfig{}

	// Construct scootapi server handler based on config.

	handler, err := parser.Create([]byte(*cfgText))
	if err != nil {
		log.Fatal("Error configuring Scoot API: ", err)
	}

	// Start API Server
	log.Println("Starting API Server")

	err = server.Serve(handler, *addr,
		thrift.NewTTransportFactory(),
		thrift.NewTBinaryProtocolFactoryDefault())
	if err != nil {
		log.Fatal("Error serving Scoot API: ", err)
	}
}
