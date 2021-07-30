package main

import (
	"fmt"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/execution"
	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common"
	"github.com/twitter/scoot/common/endpoints"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/saga/sagalogs"
	"github.com/twitter/scoot/scheduler/api"
	"github.com/twitter/scoot/scheduler/scheduler/config"
	"github.com/twitter/scoot/scheduler/server"
	"github.com/twitter/scoot/workerapi/client"
)

type servers struct {
	thrift thrift.TServer
	http   *endpoints.TwitterServer
	grpc   bazel.GRPCServer
}

func makeServers(
	thrift thrift.TServer,
	http *endpoints.TwitterServer,
	grpc bazel.GRPCServer) servers {
	return servers{thrift, http, grpc}
}

// MakeSagaLog - TODO remove saga or refactor it so this function can be moved into saga or sagalog
// the current organization leads to cyclic dependency when moving to saga or sagalogs
func MakeSagaLog(config config.SagaLogJSONConfig) (saga.SagaLog, error) {
	if config.Type == "memory" {
		return sagalogs.MakeInMemorySagaLog(time.Duration(config.ExpirationSec)*time.Second, time.Duration(config.GCIntervalSec)*time.Second), nil
	}
	if config.Type == "file" {
		return sagalogs.MakeFileSagaLog(config.Directory)
	}

	return nil, fmt.Errorf("unsupported sagalog type: %s.  No sagalog created", config.Type)
}

// StartServer construct and start scheduler service (without using ice)
func StartServer(schedulerConfig server.SchedulerConfiguration,
	sagaLogConfig config.SagaLogJSONConfig,
	workers client.WorkersClientJSONConfig,
	thriftServerTransport thrift.TServerTransport,
	statsReceiver *stats.StatsReceiver,
	workerClientTimeout time.Duration,
	endpoints *endpoints.TwitterServer,
	bazelGRPCConfig *bazel.GRPCConfig,
	persistor server.Persistor,
	durationKeyExtractorFn func(string) string,
	cluster *cluster.Cluster) error {

	thriftTransportFactory := thrift.NewTTransportFactory()

	binaryProtocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	sl, err := MakeSagaLog(sagaLogConfig)
	if err != nil {
		return err
	}
	sagaCoordinator := saga.MakeSagaCoordinator(sl)

	rf, err := client.NewWorkerThriftClient(thriftTransportFactory, binaryProtocolFactory, common.DefaultClientTimeout, workers)
	if err != nil {
		return fmt.Errorf("error creating worker thrift client (runner.Service).  Scheduler not started. %s", err)
	}

	statefulScheduler := server.NewStatefulSchedulerFromCluster(cluster, sagaCoordinator, rf, schedulerConfig, *statsReceiver, persistor, durationKeyExtractorFn)

	bazelServer := execution.MakeExecutionServer(bazelGRPCConfig, statefulScheduler, *statsReceiver)

	thriftHandler := api.NewHandler(statefulScheduler, sagaCoordinator, *statsReceiver)
	thriftServer := api.MakeServer(thriftHandler, thriftServerTransport, thriftTransportFactory, binaryProtocolFactory)

	servers := makeServers(thriftServer, endpoints, bazelServer)

	errCh := make(chan error)
	go func() {
		errCh <- servers.http.Serve()
	}()
	go func() {
		errCh <- servers.thrift.Serve()
	}()
	go func() {
		errCh <- servers.grpc.Serve()
	}()
	log.Fatal("Error serving: ", <-errCh)

	return nil
}
