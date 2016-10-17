package main

// go:generate go-bindata -pkg "config" -o ./config/config.go config

import (
	"flag"
	"github.com/scootdev/scoot/workerapi/server"
)

var thriftAddr = flag.String("thrift_addr", "localhost:9090", "port to serve thrift on")
var httpAddr = flag.String("http_addr", "localhost:9091", "port to serve http on")

func main() {
	flag.Parse()

	bag, schema := server.Defaults()
	server.RunServer(bag, schema, nil, *thriftAddr, *httpAddr)
}
