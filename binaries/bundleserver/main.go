package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/snapshot/bundlestore"
)

func main() {
	bsAddr := flag.String("bs_addr", "localhost:11100", "addr to serve bundleserver on")
	obsAddr := flag.String("obs_addr", "localhost:11101", "addr to serve observability on")
	var configFlag = flag.String("config", "{}", "Bundle Server Config (either a filename like local.local or JSON text")
	flag.Parse()

	asset := func(s string) ([]byte, error) {
		return []byte(""), fmt.Errorf("no config files: %s", s)
	}

	configText, err := jsonconfig.GetConfigText(*configFlag, asset)
	if err != nil {
		log.Fatal(err)
	}

	bag, schema := bundlestore.Defaults()
	bag.PutMany(
		func() bundlestore.Addr { return bundlestore.Addr(*bsAddr) },
		func(s stats.StatsReceiver) *endpoints.TwitterServer {
			return endpoints.NewTwitterServer(*obsAddr, s, nil)
		},
	)

	bundlestore.RunServer(bag, schema, configText)
}
