package starter

import (
	"fmt"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/execution"
	"github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/common/endpoints"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/scheduler/api"
	"github.com/twitter/scoot/scheduler/scheduler/config"
	"github.com/twitter/scoot/scheduler/server"
	"github.com/twitter/scoot/worker/client"
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
	clusterIn *cluster.Cluster) error {

	thriftTransportFactory := thrift.NewTTransportFactory()

	binaryProtocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	var err error
	var sl saga.SagaLog
	if sl, err = MakeSagaLog(sagaLogConfig); err != nil {
		return err
	}
	sagaCoordinator := saga.MakeSagaCoordinator(sl)

	var rf func(cluster.Node) runner.Service
	if rf, err = GetWorkerRunnerServiceFn(workers, thriftTransportFactory, binaryProtocolFactory); err != nil {
		return fmt.Errorf("%s.  Scheduler not started", err)
	}

	statefulScheduler := server.NewStatefulSchedulerFromCluster(clusterIn, sagaCoordinator, rf, schedulerConfig, *statsReceiver, persistor, durationKeyExtractorFn)

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
