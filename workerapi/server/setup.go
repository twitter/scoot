package server

import (
	"github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/endpoints"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/config/jsonconfig"
	"github.com/twitter/scoot/ice"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer"
	"github.com/twitter/scoot/runner/execer/execers"
	osexec "github.com/twitter/scoot/runner/execer/os"
	"github.com/twitter/scoot/workerapi/gen-go/worker"
)

type servers struct {
	thrift thrift.TServer
	http   *endpoints.TwitterServer
}

func makeServers(thrift thrift.TServer, http *endpoints.TwitterServer) servers {
	return servers{thrift, http}
}

// Module returns a module that supports serving Thrift and HTTP
func Module() ice.Module {
	return module{}
}

type module struct{}

// Install installs functions for serving Thrift and HTTP
func (m module) Install(b *ice.MagicBag) {
	b.PutMany(
		func() thrift.TTransportFactory {
			return thrift.NewTTransportFactory()
		},
		func() thrift.TProtocolFactory {
			return thrift.NewTBinaryProtocolFactoryDefault()
		},
		func() execer.Memory {
			return 0
		},
		func(m execer.Memory, s stats.StatsReceiver) execer.Execer {
			return execers.MakeSimExecerInterceptor(execers.NewSimExecer(), osexec.NewBoundedExecer(m, s))
		},
		func(stat stats.StatsReceiver, r runner.Service) worker.Worker {
			return NewHandler(stat, r)
		},
		func(
			handler worker.Worker,
			transport thrift.TServerTransport,
			transportFactory thrift.TTransportFactory,
			protocolFactory thrift.TProtocolFactory) thrift.TServer {
			return MakeServer(handler, transport, transportFactory, protocolFactory)
		},
		makeServers,
	)
}

// Starts the Server based on the MagicBag and config schema provided
// this method blocks until the server completes running or an
// exception occurs.
func RunServer(
	bag *ice.MagicBag,
	schema jsonconfig.Schema,
	config []byte) {

	log.Info("workerapi/server RunServer(), config is:", string(config))
	// Parse Config
	mod, err := schema.Parse(config)
	if err != nil {
		log.Fatal("Error configuring Worker: ", err)
	}

	// Initialize Objects Based on Config Settings
	bag.InstallModule(mod)

	var servers servers
	err = bag.Extract(&servers)
	if err != nil {
		log.Fatal("Error injecting servers", err)
	}

	errCh := make(chan error)
	go func() {
		errCh <- servers.http.Serve()
	}()
	go func() {
		errCh <- servers.thrift.Serve()
	}()
	log.Fatal("Error serving: ", <-errCh)
}
