package main

import (
	"flag"

	log "github.com/Sirupsen/logrus"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/dialer"
	"github.com/scootdev/scoot/common/log/hooks"
	"github.com/scootdev/scoot/workerapi/client"
)

// CLI binary to talk to Workerserver
//  Supported commands: (see "-h" for all details)
//      queryworker
//      run [command]
//      abort [run ID]
//  Global flags:
//      --addr [<host:port> of workerserver]

func main() {
	log.AddHook(hooks.NewContextHook())

	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
	flag.Parse()

	level, err := log.ParseLevel(*logLevelFlag)
	if err != nil {
		log.Error(err)
		return
	}
	log.SetLevel(level)

	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	di := dialer.NewSimpleDialer(transportFactory, protocolFactory)
	cl, err := client.NewSimpleCLIClient(di)
	if err != nil {
		log.Fatal("Failed to create worker CLIClient: ", err)
	}

	err = cl.Exec()
	if err != nil {
		log.Fatal("Error running workercl: ", err)
	}
}
