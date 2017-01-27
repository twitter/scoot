package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot/bundlestore"
	"github.com/scootdev/scoot/snapshot/git/gitdb"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

func main() {
	httpAddr := flag.String("http_addr", "localhost:11101", "http addr to serve on")
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

	bag := ice.NewMagicBag()
	schema := jsonconfig.EmptySchema()
	bag.InstallModule(temp.Module())
	bag.InstallModule(gitdb.Module())
	bag.InstallModule(bundlestore.Module())
	bag.InstallModule(snapshots.Module())
	bag.InstallModule(endpoints.Module())
	bag.PutMany(
		func() endpoints.StatScope { return "apiserver" },
		func() endpoints.Addr { return endpoints.Addr(*httpAddr) },
		func(bs *bundlestore.Server, vs *snapshots.ViewServer) map[string]http.Handler {
			return map[string]http.Handler{
				"/bundle/": bs,
				// Because we don't have any stream configured,
				// for now our view server will only work for snapshots
				// in a bundle with no basis
				"/view/": vs,
			}
		},
	)
	endpoints.RunServer(bag, schema, configText)
}
