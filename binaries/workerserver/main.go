package main

// go:generate go-bindata -pkg "config" -o ./config/config.go config

import (
	"flag"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/workerapi/server"
)

var thriftAddr = flag.String("thrift_addr", "localhost:9090", "port to serve thrift on")
var httpAddr = flag.String("http_addr", "localhost:9091", "port to serve http on")

func main() {
	flag.Parse()

	bag, schema := server.Defaults()
	bag.PutMany(
		func() endpoints.StatScope { return "workerserver" },
		func() (thrift.TServerTransport, error) { return thrift.NewTServerSocket(*thriftAddr) },
		func(s stats.StatsReceiver) *endpoints.TwitterServer {
			return endpoints.NewTwitterServer(*httpAddr, s)
		},
	)
	server.RunServer(bag, schema, nil)
}
