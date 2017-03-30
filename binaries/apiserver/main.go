package main

import (
	"flag"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"net/http"
	"time"

	"github.com/scootdev/scoot/cloud/cluster"
	"github.com/scootdev/scoot/cloud/cluster/local"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/scootapi"
	"github.com/scootdev/scoot/snapshot/bundlestore"
	"github.com/scootdev/scoot/snapshot/git/gitdb"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

func main() {
	httpAddr := flag.String("http_addr", scootapi.DefaultApiBundlestore_HTTP, "'host:port' addr to serve http on")
	configFlag := flag.String("config", "{}", "API Server Config (either a filename like local.local or JSON text")
	flag.Parse()

	// log.SetFlags(log.LstdFlags | log.LUTC | log.Lshortfile)

	// The same config will be used for both bundlestore and frontend (TODO: frontend).
	asset := func(s string) ([]byte, error) {
		return []byte(""), fmt.Errorf("no config files: %s", s)
	}
	configText, err := jsonconfig.GetConfigText(*configFlag, asset)
	if err != nil {
		log.Fatal(err)
	}

	type StoreAndHandler struct {
		store    bundlestore.Store
		handler  http.Handler
		endpoint string
	}

	bag := ice.NewMagicBag()
	schema := jsonconfig.EmptySchema()
	bag.InstallModule(gitdb.Module())
	bag.InstallModule(temp.Module())
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
		func(fileStore *bundlestore.FileStore, stat stats.StatsReceiver, tmp *temp.TempDir) (*StoreAndHandler, error) {
			cfg := &bundlestore.GroupcacheConfig{
				Name:         "apiserver",
				Memory_bytes: 2 * 1024 * 1024 * 1024, //2GB
				AddrSelf:     *httpAddr,
				Endpoint:     "/groupcache",
				Cluster:      createCluster(),
			}
			store, handler, err := bundlestore.MakeGroupcacheStore(fileStore, cfg, stat)
			if err != nil {
				return nil, err
			}
			return &StoreAndHandler{store, handler, cfg.Endpoint + cfg.Name + "/"}, nil
		},
		func(sh *StoreAndHandler) bundlestore.Store {
			return sh.store
		},
	)
	endpoints.RunServer(bag, schema, configText)
}

func createCluster() *cluster.Cluster {
	f := local.MakeFetcher("apiserver", "http_addr")
	nodes, _ := f.Fetch()
	updates := cluster.MakeFetchCron(f, time.NewTicker(time.Duration(1*time.Second)).C)
	return cluster.NewCluster(nodes, updates)
}
