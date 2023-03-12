package main

import (
	"flag"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"

	"github.com/wisechengyi/scoot/common"
	"github.com/wisechengyi/scoot/common/endpoints"
	"github.com/wisechengyi/scoot/common/log/hooks"
	"github.com/wisechengyi/scoot/scheduler"
	"github.com/wisechengyi/scoot/scheduler/scheduler/config"
	"github.com/wisechengyi/scoot/scheduler/starter"
)

func nopDurationKeyExtractor(id string) string {
	return id
}

func main() {
	log.AddHook(hooks.NewContextHook())

	// Set Flags Needed by this Server
	thriftAddr := flag.String("thrift_addr", scheduler.DefaultSched_Thrift, "Bind address for api server")
	httpAddr := flag.String("http_addr", scheduler.DefaultSched_HTTP, "Bind address for http server")
	configFlag := flag.String("config", "local.memory", "Scheduler Config (either a filename like local.memory or JSON text")
	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
	flag.Parse()

	level, err := log.ParseLevel(*logLevelFlag)
	if err != nil {
		log.Fatal(err)
	}
	log.SetLevel(level)

	schedulerJSONConfigs, err := config.GetSchedulerConfigs(*configFlag)
	if err != nil {
		log.Fatalf("error parsing schedule server config.  Scheduler not started. %s", err)
	}
	schedulerConfig, err := schedulerJSONConfigs.Scheduler.CreateSchedulerConfig()
	if err != nil {
		log.Fatalf("error creating schedule server config.  Scheduler not started. %s", err)
	}

	thriftServerSocket, err := thrift.NewTServerSocket(*thriftAddr)
	if err != nil {
		log.Fatalf("error creating thrift server socket.  Scheduler not started. %s", err)
	}

	statsReceiver := endpoints.MakeStatsReceiver("scheduler").Precision(time.Millisecond)
	httpServer := endpoints.NewTwitterServer(endpoints.Addr(*httpAddr), statsReceiver, nil)

	nodesUpdatesCh, err := starter.StartCluster(schedulerJSONConfigs.Cluster)
	if err != nil {
		log.Fatalf("%s. Scheduler not started", err)
	}

	log.Infof("Starting Cloud Scoot API Server & Scheduler on %s with %s", *thriftAddr, *configFlag)
	starter.StartServer(*schedulerConfig, schedulerJSONConfigs.SagaLog, schedulerJSONConfigs.Workers,
		thriftServerSocket, &statsReceiver, common.DefaultClientTimeout, httpServer,
		nil, nopDurationKeyExtractor, nodesUpdatesCh)
}
