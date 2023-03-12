package main

import (
	"github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"
	"github.com/wisechengyi/scoot/common"
	"github.com/wisechengyi/scoot/common/dialer"
	"github.com/wisechengyi/scoot/common/log/hooks"
	"github.com/wisechengyi/scoot/scheduler/client/cli"
)

// CLI binary to talk to Cloud Scoot API
//	Supported commands: (see "-h" for all options)
//		run_job [command]
// 		get_job_status [job id]
//		watch_job [job id]
//		run_smoke_test
//		get_scheduler_status
//		set_scheduler_status [limit]
//		get_scheduling_alg_params
//		set_scheduling_alg_params
//	Global flags:
//		--addr [<host:port> of cloud server]
// 		--log_level [<error|info|debug> level and above should be logged]

func main() {
	log.AddHook(hooks.NewContextHook())

	transportFactory := thrift.NewTTransportFactory()
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	di := dialer.NewSimpleDialer(transportFactory, protocolFactory, common.DefaultClientTimeout)
	cl, err := cli.NewSimpleCLIClient(di)
	if err != nil {
		log.Fatal("Failed to create new scoot CLI client: ", err)
	}

	err = cl.Exec()
	if err != nil {
		log.Fatal("Error running scootcl ", err)
	}
}
