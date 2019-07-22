package main

//go:generate go-bindata -pkg "config" -o ./config/config.go config
//go:generate go fmt ./config/config.go

import (
	"flag"

	"github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/binaries/scheduler/config"
	"github.com/twitter/scoot/common/endpoints"
	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/config/jsonconfig"
	"github.com/twitter/scoot/config/scootconfig"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/scootapi"
	"github.com/twitter/scoot/scootapi/server"
)

func main() {
	log.AddHook(hooks.NewContextHook())

	// Set Flags Needed by this Server
	thriftAddr := flag.String("thrift_addr", scootapi.DefaultSched_Thrift, "Bind address for api server")
	httpAddr := flag.String("http_addr", scootapi.DefaultSched_HTTP, "Bind address for http server")
	grpcAddr := flag.String("grpc_addr", scootapi.DefaultSched_GRPC, "Bind address for grpc server")
	configFlag := flag.String("config", "local.memory", "Scheduler Config (either a filename like local.memory or JSON text")
	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
	grpcConns := flag.Int("max_grpc_conn", bazel.MaxSimultaneousConnections, "max grpc listener connections")
	grpcRate := flag.Int("max_grpc_rps", bazel.MaxRequestsPerSecond, "max grpc incoming requests per second")
	grpcBurst := flag.Int("max_grpc_rps_burst", bazel.MaxRequestsBurst, "max grpc incoming requests burst")
	grpcStreams := flag.Int("max_grpc_streams", bazel.MaxConcurrentStreams, "max grpc streams per client")
	grpcIdleMins := flag.Int("max_grpc_idle_mins", bazel.MaxConnIdleMins, "max grpc connection idle time")
	flag.Parse()

	level, err := log.ParseLevel(*logLevelFlag)
	if err != nil {
		log.Error(err)
		return
	}
	log.SetLevel(level)

	configText, err := jsonconfig.GetConfigText(*configFlag, config.Asset)
	if err != nil {
		log.Fatal(err)
	}
	bag, schema := server.Defaults()
	bag.PutMany(
		func() (thrift.TServerTransport, error) {
			return thrift.NewTServerSocket(*thriftAddr)
		},

		func() scootconfig.ClientTimeout {
			return scootconfig.ClientTimeout(scootconfig.DefaultClientTimeout)
		},

		func(s stats.StatsReceiver) *endpoints.TwitterServer {
			return endpoints.NewTwitterServer(endpoints.Addr(*httpAddr), s, nil)
		},

		func() *bazel.GRPCConfig {
			return &bazel.GRPCConfig{
				GRPCAddr:          *grpcAddr,
				ListenerMaxConns:  *grpcConns,
				RateLimitPerSec:   *grpcRate,
				BurstLimitPerSec:  *grpcBurst,
				ConcurrentStreams: *grpcStreams,
				MaxConnIdleMins:   *grpcIdleMins,
			}
		},

		func() (*temp.TempDir, error) {
			return temp.NewTempDir("", "sched")
		},
	)

	log.Info("Starting Cloud Scoot API Server & Scheduler on", *thriftAddr)
	server.RunServer(bag, schema, configText)
}
