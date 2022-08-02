package starter

import (
	"fmt"
	"net/http"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/endpoints"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer"
	"github.com/twitter/scoot/runner/execer/execers"
	osexec "github.com/twitter/scoot/runner/execer/os"
	"github.com/twitter/scoot/runner/runners"
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/git/gitdb"
)

// WorkerStatusHTTPHandler implements http.Handler to return status for aurora health check endpoints
// based on the underlying runner.ServiceStatus
type WorkerStatusHTTPHandler struct {
	svc runner.Service
}

func (h *WorkerStatusHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, svcStatus, err := h.svc.QueryNow(runner.Query{})
	if err == nil {
		if !svcStatus.Initialized {
			fmt.Fprintf(w, "not initialized")
			return
		}
		if !svcStatus.IsHealthy {
			fmt.Fprintf(w, "not healthy")
			return
		}
		fmt.Fprintf(w, "ok")
		return
	}
	fmt.Fprintf(w, err.Error())
}

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
func StartServer(
	thriftAddr string,
	httpAddr string,
	db *gitdb.DB,
	oc runners.HttpOutputCreator,
	rID runner.RunnerID,
	dirMonitor *stats.DirsMonitor,
	memCap uint64,
	stat *stats.StatsReceiver,
	preprocessors []func() error,
	postprocessors []func() error,
	memUtilizationFunc func() (int64, error),
	uploader runners.LogUploader,
) {
	// create worker object:
	// worker support objects
	memory := execer.Memory(memCap)
	execer := execers.MakeSimExecerInterceptor(execers.NewSimExecer(), osexec.NewBoundedExecer(memory, memUtilizationFunc, *stat))

	var filerMap runner.RunTypeMap = runner.MakeRunTypeMap()
	if db != nil {
		gitFiler := snapshot.NewDBAdapter(db)
		filerMap[runner.RunTypeScoot] = snapshot.FilerAndInitDoneCh{Filer: gitFiler, IDC: db.InitDoneCh}
	}
	// the worker object
	worker := runners.NewSingleRunner(execer, filerMap, oc, *stat, dirMonitor, rID, preprocessors, postprocessors, uploader)

	// add service wrappers
	// thrift wrapper
	transport, err := thrift.NewTServerSocket(thriftAddr)
	if err != nil {
		log.Fatalf("couldn't create thrift transport:%s", err)
	}
	thriftTransportFactory := thrift.NewTTransportFactory()
	binaryProtocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	handler := NewHandler(*stat, worker)
	thriftServer := MakeServer(handler, transport, thriftTransportFactory, binaryProtocolFactory)

	// http wrapper
	handlers := map[string]http.Handler{
		oc.HttpPath(): oc,
		"/health":     &WorkerStatusHTTPHandler{svc: worker},
	}
	httpServer := endpoints.NewTwitterServer(endpoints.Addr(httpAddr), *stat, handlers)

	// start the servers listening on their endpoints
	servers := makeServers(thriftServer, httpServer)
	errCh := make(chan error)
	go func() {
		errCh <- servers.http.Serve()
	}()
	go func() {
		errCh <- servers.thrift.Serve()
	}()
	log.Fatal("Error serving: ", <-errCh)
}

func GetStatsReceiver() stats.StatsReceiver {
	return endpoints.MakeStatsReceiver("workerserver").Precision(time.Millisecond)
}
