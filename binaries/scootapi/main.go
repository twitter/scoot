package main

import (
	"flag"

	log "github.com/Sirupsen/logrus"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/dialer"
	"github.com/scootdev/scoot/common/log/hooks"
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
// 		--log_level [<error|info|debug> level and above should be logged]

func main() {
	log.AddHook(hooks.NewContextHook())

	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
	flag.Parse()

	level, err := log.ParseLevel(*logLevelFlag)
	if err != nil {
		log.Error(err)
		level = log.InfoLevel
	}
	log.SetLevel(level)

	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	di := dialer.NewSimpleDialer(transportFactory, protocolFactory)
	cl, err := client.NewSimpleCLIClient(di)
	if err != nil {
		log.Fatal("Failed to create new ScootAPI CLI client: ", err)
	}

	err = cl.Exec()
	if err != nil {
		log.Fatal("Error running scootapi ", err)
	}
}
