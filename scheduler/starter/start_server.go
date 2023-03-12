package starter

import (
	"fmt"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"

	"github.com/wisechengyi/scoot/cloud/cluster"
	"github.com/wisechengyi/scoot/common/endpoints"
	"github.com/wisechengyi/scoot/common/stats"
	"github.com/wisechengyi/scoot/runner"
	"github.com/wisechengyi/scoot/saga"
	"github.com/wisechengyi/scoot/scheduler/api"
	"github.com/wisechengyi/scoot/scheduler/scheduler/config"
	"github.com/wisechengyi/scoot/scheduler/server"
	"github.com/wisechengyi/scoot/worker/client"
)

type servers struct {
	thrift thrift.TServer
	http   *endpoints.TwitterServer
}

func makeServers(
	thrift thrift.TServer,
	http *endpoints.TwitterServer) servers {
	return servers{thrift, http}
}

// StartServer construct and start scheduler service (without using ice)
func StartServer(schedulerConfig server.SchedulerConfiguration,
	sagaLogConfig config.SagaLogJSONConfig,
	workers client.WorkersClientJSONConfig,
	thriftServerTransport thrift.TServerTransport,
	statsReceiver *stats.StatsReceiver,
	workerClientTimeout time.Duration,
	endpoints *endpoints.TwitterServer,
	persistor server.Persistor,
	durationKeyExtractorFn func(string) string,
	nodesUpdatesCh chan []cluster.NodeUpdate) error {

	thriftTransportFactory := thrift.NewTTransportFactory()

	binaryProtocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	var err error
	var sl saga.SagaLog
	if sl, err = MakeSagaLog(sagaLogConfig); err != nil {
		return err
	}
	sagaCoordinator := saga.MakeSagaCoordinator(sl, *statsReceiver)

	var rf func(cluster.Node) runner.Service
	if rf, err = GetWorkerRunnerServiceFn(workers, thriftTransportFactory, binaryProtocolFactory); err != nil {
		return fmt.Errorf("%s.  Scheduler not started", err)
	}

	statefulScheduler := server.NewStatefulScheduler(nodesUpdatesCh, sagaCoordinator, rf, schedulerConfig, *statsReceiver, persistor, durationKeyExtractorFn)

	thriftHandler := api.NewHandler(statefulScheduler, sagaCoordinator, *statsReceiver)
	thriftServer := api.MakeServer(thriftHandler, thriftServerTransport, thriftTransportFactory, binaryProtocolFactory)

	servers := makeServers(thriftServer, endpoints)

	errCh := make(chan error)
	go func() {
		errCh <- servers.http.Serve()
	}()
	go func() {
		errCh <- servers.thrift.Serve()
	}()
	log.Fatal("Error serving: ", <-errCh)

	return nil
}
