package main

import (
	log "github.com/inconshreveable/log15"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/dialer"
	"github.com/scootdev/scoot/scootapi/client"
)

// CLI binary to talk to Cloud Scoot API
//	Supported commands: (see "-h" for all options)
//		run_job [command]
// 		get_job_status [job id]
//		watch_job [job id]
//		run_smoke_test
//	Global flags:
//		--addr [<host:port> of cloud server]

func main() {
	// log.SetFlags(log.LstdFlags | log.LUTC | log.Lshortfile)

	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	di := dialer.NewSimpleDialer(transportFactory, protocolFactory)
	cl, err := client.NewSimpleCLIClient(di)
	if err != nil {
		log.Crit("Failed to create new ScootAPI CLI client: ", err)
	}

	err = cl.Exec()
	if err != nil {
		log.Crit("Error running scootapi ", err)
	}
}
