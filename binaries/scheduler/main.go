package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/cloud/cluster/local"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/sched/config"
	"github.com/scootdev/scoot/scootapi/server"
)

var addr = flag.String("addr", "localhost:9090", "Bind address for api server.")
var httpPort = flag.Int("http_port", 9091, "port to serve http on")
var cfgText = flag.String("sched_config", "", "Scheduler Configuration.")

func main() {
	log.Println("Starting Cloud Scoot API Server & Scheduler")
	flag.Parse()
	stat, _ := stats.NewCustomStatsReceiver(stats.NewFinagleStatsRegistry, 15*time.Second)
	stat = stat.Precision(time.Millisecond)
	endpoints.RegisterStats("/admin/metrics.json", stat)
	endpoints.RegisterHealthCheck("/")
	go endpoints.Serve(fmt.Sprintf("localhost:%d", *httpPort))

	parser := config.DefaultParser()
	parser.Workers[""] = &config.RPCWorkersConfig{Type: "rpc"}
	parser.Cluster[""] = &local.ClusterLocalConfig{Type: "local"}
	parser.Cluster["local"] = &local.ClusterLocalConfig{}

	// Construct scootapi server handler based on config.

	handler, err := parser.Create([]byte(*cfgText), stat)
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
