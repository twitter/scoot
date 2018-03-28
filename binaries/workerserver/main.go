package main

//go:generate go-bindata -pkg "config" -o ./config/config.go config
//go:generate go fmt ./config/config.go

import (
	"flag"
	"math/rand"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/binaries/workerserver/config"
	"github.com/twitter/scoot/cloud/cluster/local"
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/common/endpoints"
	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/config/jsonconfig"
	"github.com/twitter/scoot/ice"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer"
	"github.com/twitter/scoot/runner/runners"
	"github.com/twitter/scoot/scootapi"
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/bazel"
	"github.com/twitter/scoot/snapshot/bundlestore"
	"github.com/twitter/scoot/snapshot/git/gitdb"
	"github.com/twitter/scoot/snapshot/git/repo"
	"github.com/twitter/scoot/snapshot/store"
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
	casAddr := flag.String("cas_addr", "", "'host:port' of a server supporting CAS API over GRPC")
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
	bag.InstallModule(bazel.Module())
	bag.PutMany(
		func() endpoints.StatScope { return "workerserver" },
		func() endpoints.Addr { return endpoints.Addr(*httpAddr) },
		func() (thrift.TServerTransport, error) { return thrift.NewTServerSocket(*thriftAddr) },
		func() (*repo.Repository, error) {
			return repo.NewRepository(*repoDir)
		},
		func(tmp *temp.TempDir) (runners.HttpOutputCreator, error) {
			outDir, err := tmp.FixedDir("output")
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
		func() server.DriveTypeFunc {
			// Note: this returns 1 (HDD) if there's an error running the cmd, the file doesn't exist, or if the file
			// doesn't contain only "0"
			// TODO: make this cmd configurable for use on different OS's (e.g. system_profiler SPSerialATADataType | grep SSD)
			b, _ := exec.Command("cat", "/sys/block/sda/queue/rotational").Output()
			f := func() int64 {
				if strings.TrimSpace(string(b)) == "0" {
					return 0
				}
				return int64(1)
			}
			return server.DriveTypeFunc(f)
		},
		// Use storeHandle if provided, else try Fetching, then GetScootApiAddr(), then fallback to tmp file store.
		func(tmp *temp.TempDir) (store.Store, error) {
			if *storeHandle != "" {
				if strings.HasPrefix(*storeHandle, "/") {
					return store.MakeFileStoreInTemp(&temp.TempDir{Dir: *storeHandle})
				} else {
					return store.MakeHTTPStore(scootapi.APIAddrToBundlestoreURI(*storeHandle)), nil
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
				return store.MakeHTTPStore(scootapi.APIAddrToBundlestoreURI(storeAddr)), nil
			}
			log.Info("No stores specified or found, creating a tmp file store")
			return store.MakeFileStoreInTemp(tmp)
		},
		// Create BzFiler to handle Bazel API requests
		func(tmp *temp.TempDir) (*bazel.BzFiler, error) {
			addr := ""
			if *casAddr != "" {
				addr = *casAddr
			} else {
				nodes, _ := local.MakeFetcher("apiserver", "grpc_addr").Fetch()
				if len(nodes) > 0 {
					r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
					addr = string(nodes[r.Intn(len(nodes))].Id())
					log.Info("No grpc cas servers specified, but successfully fetched apiserver addr: ", nodes, " --> ", addr)
				}
			}
			resolver := dialer.NewConstantResolver(addr)
			return bazel.MakeBzFiler(tmp, resolver)
		},
		// Initialize map of Filers w/ init chans based on RunTypes
		// GitDB is created from its ice module defaults and handles Scoot API requests
		func(gitDB *gitdb.DB, bzFiler *bazel.BzFiler) runner.RunTypeMap {
			gitFiler := snapshot.NewDBAdapter(gitDB)

			var filerMap runner.RunTypeMap = runner.MakeRunTypeMap()
			filerMap[runner.RunTypeScoot] = snapshot.FilerAndInitDoneCh{Filer: gitFiler, IDC: gitDB.InitDoneCh}
			filerMap[runner.RunTypeBazel] = snapshot.FilerAndInitDoneCh{Filer: bzFiler, IDC: nil}
			return filerMap
		},
	)

	log.Info("Serving thrift on", *thriftAddr) //It's hard to access the thriftAddr value downstream, print it here.
	server.RunServer(bag, schema, configText)
}
