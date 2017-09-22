package main

//go:generate go-bindata -pkg "config" -o ./config/config.go config
//go:generate go fmt ./config/config.go

import (
	"flag"
	"math/rand"
	"net/http"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/twitter/scoot/binaries/workerserver/config"
	"github.com/twitter/scoot/cloud/cluster/local"
	"github.com/twitter/scoot/common/endpoints"
	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/config/jsonconfig"
	"github.com/twitter/scoot/ice"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer"
	"github.com/twitter/scoot/runner/runners"
	"github.com/twitter/scoot/scootapi"
	"github.com/twitter/scoot/snapshot/bundlestore"
	"github.com/twitter/scoot/snapshot/git/gitdb"
	"github.com/twitter/scoot/snapshot/git/repo"
	"github.com/twitter/scoot/workerapi/server"
)

func main() {
	log.AddHook(hooks.NewContextHook())

	thriftAddr := flag.String("thrift_addr", scootapi.DefaultWorker_Thrift, "addr to serve thrift on")
	httpAddr := flag.String("http_addr", scootapi.DefaultWorker_HTTP, "addr to serve http on")
	configFlag := flag.String("config", "local.local", "Worker Server Config (either a filename like local.local or JSON text")
	memCapFlag := flag.Uint64("mem_cap", 0, "Kill runs that exceed this amount of memory, in bytes. Zero means no limit.")
	repoDir := flag.String("repo", "", "Abs dir path to a git repo to run against (don't use important repos yet!).")
	storeHandle := flag.String("bundlestore", "", "Abs file path or an http 'host:port' to store/get bundles.")
	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
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

	bag := ice.NewMagicBag()
	schema := jsonconfig.EmptySchema()
	bag.InstallModule(temp.Module())
	bag.InstallModule(gitdb.Module())
	bag.InstallModule(bundlestore.Module())
	bag.InstallModule(endpoints.Module())
	bag.InstallModule(runners.Module())
	bag.InstallModule(server.Module())
	bag.PutMany(
		func() endpoints.StatScope { return "workerserver" },
		func() endpoints.Addr { return endpoints.Addr(*httpAddr) },
		func() (thrift.TServerTransport, error) { return thrift.NewTServerSocket(*thriftAddr) },
		func() (*repo.Repository, error) {
			return repo.NewRepository(*repoDir)
		},
		func(tmpDir *temp.TempDir) (runners.HttpOutputCreator, error) {
			outDir, err := tmpDir.FixedDir("output")
			if err != nil {
				return nil, err
			}
			return runners.NewHttpOutputCreator(outDir, ("http://" + *httpAddr + "/output/"))
		},
		func(oc runners.HttpOutputCreator) runner.OutputCreator {
			return oc
		},
		func(outputCreator runners.HttpOutputCreator) map[string]http.Handler {
			return map[string]http.Handler{outputCreator.HttpPath(): outputCreator}
		},
		func() execer.Memory {
			return execer.Memory(*memCapFlag)
		},
		// Use storeHandle if provided, else try Fetching, then GetScootApiAddr(), then fallback to tmp file store.
		func(tmp *temp.TempDir) (bundlestore.Store, error) {
			if *storeHandle != "" {
				if strings.HasPrefix(*storeHandle, "/") {
					return bundlestore.MakeFileStoreInTemp(&temp.TempDir{Dir: *storeHandle})
				} else {
					return bundlestore.MakeHTTPStore(scootapi.APIAddrToBundlestoreURI(*storeHandle)), nil
				}
			}
			storeAddr := ""
			nodes, _ := local.MakeFetcher("apiserver", "http_addr").Fetch()
			if len(nodes) > 0 {
				r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
				storeAddr = string(nodes[r.Intn(len(nodes))].Id())
				log.Info("No stores specified, but successfully fetched store addr: ", nodes, " --> ", storeAddr)
			} else {
				_, storeAddr, _ = scootapi.GetScootapiAddr()
				log.Info("No stores specified, but successfully read .cloudscootaddr: ", storeAddr)
			}
			if storeAddr != "" {
				return bundlestore.MakeHTTPStore(scootapi.APIAddrToBundlestoreURI(storeAddr)), nil
			}
			log.Info("No stores specified or found, creating a tmp file store")
			return bundlestore.MakeFileStoreInTemp(tmp)
		},
	)

	log.Info("Serving thrift on", *thriftAddr) //It's hard to access the thriftAddr value downstream, print it here.
	server.RunServer(bag, schema, configText)
}
