package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/common/dialer"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/scootapi"
	"github.com/scootdev/scoot/scootapi/frontend"
	"github.com/scootdev/scoot/snapshot/bundlestore"
)

func main() {
	addr := flag.String("addr", "localhost:11100", "thrift addr to serve scootapi frontend on")
	bsAddr := flag.String("bs_addr", "localhost:11101", "http addr to serve bundlestore on")
	obsAddr := flag.String("obs_addr", "localhost:11102", "http addr to serve observability stats on")
	backendAddr := flag.String("backend_addr", "localhost:9090", "thrift addr that this frontend delegates to")
	configFlag := flag.String("config", "{}", "API Server Config (either a filename like local.local or JSON text")
	flag.Parse()

	// The same config will be used for both bundlestore and frontend.
	asset := func(s string) ([]byte, error) {
		return []byte(""), fmt.Errorf("no config files: %s", s)
	}
	configText, err := jsonconfig.GetConfigText(*configFlag, asset)
	if err != nil {
		log.Fatal(err)
	}

	// Start a new goroutine for bundlestore server as well as the observability server.
	bag, schema := bundlestore.Defaults()
	bag.PutMany(
		func() bundlestore.Addr { return bundlestore.Addr(*bsAddr) },
		func(s stats.StatsReceiver) *endpoints.TwitterServer {
			return endpoints.NewTwitterServer(*obsAddr, s, nil)
		},
		func(tmp *temp.TempDir) (bundlestore.Store, error) {
			return bundlestore.MakeFileStoreInTemp(tmp)
		},
		func() *scootapi.CloudScootClient {
			transportFactory := thrift.NewTTransportFactory()
			protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
			return scootapi.NewCloudScootClient(
				scootapi.CloudScootClientConfig{
					Addr:   *backendAddr,
					Dialer: dialer.NewSimpleDialer(transportFactory, protocolFactory),
				})
		},
	)
	go bundlestore.RunServer(bag, schema, configText)

	// Start a scheduler frontend in the current goroutine and skip the observability server.
	bag, schema = frontend.Defaults()
	bag.PutMany(
		func() (thrift.TServerTransport, error) { return thrift.NewTServerSocket(*addr) },
	)
	frontend.RunServer(bag, schema, configText)
}
