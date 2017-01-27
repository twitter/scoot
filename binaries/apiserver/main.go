package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/scootdev/scoot/cloud/cluster/local"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/scootapi"
	"github.com/scootdev/scoot/snapshot/bundlestore"
)

func main() {
	bundlestoreAddr := flag.String("bundlestore_addr", scootapi.DefaultApiBundlestore_HTTP, "http addr to serve bundlestore on")
	statsAddr := flag.String("stats_addr", scootapi.DefaultApiStats_HTTP, "http addr to serve observability stats on")
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

	type storeAndHandler struct {
		store    bundlestore.Store
		handler  http.Handler
		endpoint string
	}

	// Start a new goroutine for bundlestore server as well as the observability server.
	bag, schema := bundlestore.Defaults()
	bag.PutMany(
		func() bundlestore.Addr { return bundlestore.Addr(*bundlestoreAddr) },
		func(s stats.StatsReceiver, sh *storeAndHandler) *endpoints.TwitterServer {
			handlers := map[string]http.Handler{sh.endpoint: sh.handler}
			return endpoints.NewTwitterServer(*statsAddr, s, handlers)
		},
		func(tmp *temp.TempDir) (*storeAndHandler, error) {
			//TODO: refactor cluster config and fetcher to remove worker refs and make them more generic.
			cfg := &bundlestore.GroupcacheConfig{
				Name:         "apiserver",
				Memory_bytes: 2 * 1024 * 1024 * 1024, //2GB
				AddrSelf:     *bundlestoreAddr,
				Endpoint:     "/groupcache",
				Fetcher:      local.MakeFetcher("apiserver", "bundlestore_addr"),
			}
			if fileStore, err := bundlestore.MakeFileStoreInTemp(tmp); err != nil {
				return nil, err
			} else {
				store, handler, err := bundlestore.MakeGroupcacheStore(fileStore, cfg)
				return &storeAndHandler{store, handler, cfg.Endpoint}, err
			}
		},
		func(sh *storeAndHandler) bundlestore.Store {
			return sh.store
		},
	)

	bundlestore.RunServer(bag, schema, configText)
}
