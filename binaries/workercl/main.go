package main

import (
	"flag"

	log "github.com/sirupsen/logrus"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/config/scootconfig"
	"github.com/twitter/scoot/workerapi/client"
)

// CLI binary to talk to Workerserver
//  Supported commands: (see "-h" for all details)
//      queryworker
//      run [command]
//      abort [run ID]
//  Global flags:
//      --addr [<host:port> of workerserver]
// 		--log_level [<error|info|debug> level and above should be logged]

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

	di := dialer.NewSimpleDialer(transportFactory, protocolFactory, scootconfig.DefaultClientTimeout)
	cl, err := client.NewSimpleCLIClient(di)
	if err != nil {
		log.Fatal("Failed to create worker CLIClient: ", err)
	}

	err = cl.Exec()
	if err != nil {
		log.Fatal("Error running workercl: ", err)
	}
}
