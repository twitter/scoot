package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot/bundlestore"
)

func main() {
	bsAddr := flag.String("bs_addr", "localhost:11101", "http addr to serve bundlestore on")
	obsAddr := flag.String("obs_addr", "localhost:11102", "http addr to serve observability stats on")
	configFlag := flag.String("config", "{}", "API Server Config (either a filename like local.local or JSON text")
	flag.Parse()

	// The same config will be used for both bundlestore and frontend (TODO: frontend).
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
	)

	bundlestore.RunServer(bag, schema, configText)
}
