package main

import (
	"flag"
	"fmt"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"

	cc "github.com/twitter/scoot/cloud/cluster"
	"github.com/twitter/scoot/cloud/cluster/local"
	"github.com/twitter/scoot/common"
	"github.com/twitter/scoot/common/endpoints"
	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/config/jsonconfig"
	"github.com/twitter/scoot/snapshot/bundlestore"
	"github.com/twitter/scoot/snapshot/git/gitdb"
	"github.com/twitter/scoot/snapshot/snapshots"
	"github.com/twitter/scoot/snapshot/store"
)

func main() {
	log.AddHook(hooks.NewContextHook())

	httpAddr := flag.String("http_addr", bundlestore.DefaultApiBundlestore_HTTP, "'host:port' addr to serve http on")
	configFlag := flag.String("config", "{}", "API Server Config (either a filename like local.local or JSON text")
	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
	cacheSize := flag.Int64("cache_size", 2*1024*1024*1024, "In-memory bundle cache size in bytes")
	flag.Parse()

	level, err := log.ParseLevel(*logLevelFlag)
	if err != nil {
		log.Error(err)
		return
	}
	log.SetLevel(level)

	// The same config will be used for both bundlestore and frontend (TODO: frontend).
	asset := func(s string) ([]byte, error) {
		return []byte(""), fmt.Errorf("no config files: %s", s)
	}
	configText, err := jsonconfig.GetConfigText(*configFlag, asset)
	if err != nil {
		log.Fatal(err)
	}

	type StoreAndHandler struct {
		store    store.Store
		handler  http.Handler
		endpoint string
	}

	bag := bundlestore.Defaults()
	schema := jsonconfig.EmptySchema()
	bag.InstallModule(gitdb.Module())
	bag.InstallModule(bundlestore.Module())
	bag.InstallModule(snapshots.Module())
	bag.InstallModule(endpoints.Module())
	bag.PutMany(
		func() endpoints.StatScope { return "apiserver" },
		func() endpoints.Addr { return endpoints.Addr(*httpAddr) },
		func(bs *bundlestore.Server, vs *snapshots.ViewServer, sh *StoreAndHandler) map[string]http.Handler {
			return map[string]http.Handler{
				"/bundle/": bs,
				// Because we don't have any stream configured,
				// for now our view server will only work for snapshots
				// in a bundle with no basis
				"/view/":    vs,
				sh.endpoint: sh.handler,
			}
		},
		func(stat stats.StatsReceiver) cc.NodeReqChType {
			f := local.MakeFetcher("apiserver", "http_addr")
			_, reqNodeCh := cc.NewCluster(stat, f, false, 1*time.Second, common.DefaultClusterChanSize)
			return reqNodeCh
		},
		func(fileStore *store.FileStore, stat stats.StatsReceiver, ttlc *store.TTLConfig, nodeReqCh cc.NodeReqChType) (*StoreAndHandler, error) {
			cfg := &store.GroupcacheConfig{
				Name:         "apiserver",
				Memory_bytes: *cacheSize,
				AddrSelf:     *httpAddr,
				Endpoint:     "/groupcache",
				NodeReqCh:    nodeReqCh,
			}
			store, handler, err := store.MakeGroupcacheStore(fileStore, cfg, ttlc, stat)
			if err != nil {
				return nil, err
			}
			return &StoreAndHandler{store, handler, cfg.Endpoint + cfg.Name + "/"}, nil
		},
		func(sh *StoreAndHandler) store.Store {
			return sh.store
		},
	)
	bundlestore.RunServer(bag, schema, configText)
}
