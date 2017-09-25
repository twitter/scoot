package main

import (
	"github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/config/scootconfig"
	"github.com/twitter/scoot/scootapi/client"
)

// CLI binary to talk to Cloud Scoot API
//	Supported commands: (see "-h" for all options)
//		run_job [command]
// 		get_job_status [job id]
//		watch_job [job id]
//		run_smoke_test
//	Global flags:
//		--addr [<host:port> of cloud server]
// 		--log_level [<error|info|debug> level and above should be logged]

func main() {
	log.AddHook(hooks.NewContextHook())

	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	di := dialer.NewSimpleDialer(transportFactory, protocolFactory, scootconfig.DefaultClientTimeout)
	cl, err := client.NewSimpleCLIClient(di)
	if err != nil {
		log.Fatal("Failed to create new ScootAPI CLI client: ", err)
	}

	err = cl.Exec()
	if err != nil {
		log.Fatal("Error running scootapi ", err)
	}
}
