package main

import (
	"github.com/scootdev/scoot/common/log"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/dialer"
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
	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	di := dialer.NewSimpleDialer(transportFactory, protocolFactory)
	cl, err := client.NewSimpleCLIClient(di)
	if err != nil {
		log.Crit("Failed to create worker CLIClient: ", err)
	}

	err = cl.Exec()
	if err != nil {
		log.Crit("Error running workercl: ", err)
	}
}
