package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner/execer/execers"
	osexec "github.com/scootdev/scoot/runner/execer/os"
	localrunner "github.com/scootdev/scoot/runner/local"
	"github.com/scootdev/scoot/snapshot/snapshots"
	"github.com/scootdev/scoot/workerapi/server"
)

var thriftPort = flag.Int("thrift_port", 9090, "port to serve thrift on")
var httpPort = flag.Int("http_port", 9091, "port to serve http on")

func main() {
	flag.Parse()
	stat := endpoints.MakeStatsReceiver("").Precision(time.Millisecond)
	twServer := endpoints.NewTwitterServer(fmt.Sprintf("localhost:%d", *httpPort), stat)
	go twServer.Serve()

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transportFactory := thrift.NewTTransportFactory()

	tempDir, err := temp.TempDirDefault()
	if err != nil {
		log.Fatal("error creating temp dir: ", err)
	}

	stats := stat.Scope("workerserver")
	outputCreator, err := localrunner.NewOutputCreator(tempDir)
	if err != nil {
		log.Fatal("Error creating OutputCreatorr: ", err)
	}

	ex := execers.MakeSimExecerInterceptor(execers.NewSimExecer(nil), osexec.NewExecer())
	run := localrunner.NewSimpleRunner(ex, snapshots.MakeTempCheckouter(tempDir), outputCreator)
	handler := server.NewHandler(stats, run)
	err = server.Serve(handler, fmt.Sprintf("localhost:%d", *thriftPort), transportFactory, protocolFactory)
	if err != nil {
		log.Fatal("Error serving Worker Server: ", err)
	}
}
