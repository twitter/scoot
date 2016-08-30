package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/runner/execer/fake"
	localrunner "github.com/scootdev/scoot/runner/local"
	fakesnaps "github.com/scootdev/scoot/snapshots/fake"
	"github.com/scootdev/scoot/workerapi/server"
)

var thriftPort = flag.Int("thrift_port", 9090, "port to serve thrift on")
var httpPort = flag.Int("http_port", 9091, "port to serve http on")

func main() {
	flag.Parse()
	stat := endpoints.MakeStatsReceiver().Precision(time.Millisecond)
	twServer := endpoints.NewTwitterServer(fmt.Sprintf("localhost:%d", *httpPort), stat)
	go twServer.Serve()

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transportFactory := thrift.NewTTransportFactory()

	stats := stat.Scope("workerserver")
	saver, err := localrunner.NewSaver()
	if err != nil {
		log.Fatal("Error creating Saver: ", err)
	}
	run := localrunner.NewSimpleRunner(fake.NewSimExecer(nil), fakesnaps.MakeInvalidCheckouter(), saver)
	version := func() string { return "" }
	handler := server.NewHandler(stats, run, version)
	err = server.Serve(handler, fmt.Sprintf("localhost:%d", *thriftPort), transportFactory, protocolFactory)
	if err != nil {
		log.Fatal("Error serving Worker Server: ", err)
	}
}
