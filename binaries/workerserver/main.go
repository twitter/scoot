package main

//go:generate go-bindata -pkg "config" -o ./config/config.go config

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/binaries/workerserver/config"
	"github.com/scootdev/scoot/common/endpoints"
	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/bundlestore"
	"github.com/scootdev/scoot/snapshot/git/gitdb"
	"github.com/scootdev/scoot/snapshot/git/repo"

	"github.com/scootdev/scoot/workerapi/server"
)

var thriftAddr = flag.String("thrift_addr", "localhost:9090", "port to serve thrift on")
var httpAddr = flag.String("http_addr", "localhost:9091", "port to serve http on")
var configFlag = flag.String("config", "local.local", "Worker Server Config (either a filename like local.local or JSON text")
var memCapFlag = flag.Uint64("mem_cap", 0, "Kill runs that exceed this amount of memory, in bytes. Zero means no limit.")
var repoDir = flag.String("repo", "", "Absolute path to a git repo to run against (don't use with important repos yet!).")
var store = flag.String("bundlestore", "", "Absolute file path or http URL where repo can download bundles from.")

func main() {
	flag.Parse()

	configText, err := jsonconfig.GetConfigText(*configFlag, config.Asset)
	if err != nil {
		log.Fatal(err)
	}

	bag, schema := server.Defaults()
	bag.PutMany(
		func() (thrift.TServerTransport, error) { return thrift.NewTServerSocket(*thriftAddr) },

		func(s stats.StatsReceiver, handlers map[string]http.Handler) *endpoints.TwitterServer {
			return endpoints.NewTwitterServer(*httpAddr, s, handlers)
		},

		func(tmpDir *temp.TempDir) (snapshot.Filer, error) {
			// Make the repo, initializing from scratch if a git dir isn't provided.
			var r *repo.Repository
			if *repoDir == "" {
				if repoTmp, err := tmpDir.TempDir("workerfiler"); err != nil {
					return nil, err
				} else if r, err = repo.InitRepo(repoTmp.Dir); err != nil {
					return nil, err
				}
			} else {
				r, err = repo.NewRepository(*repoDir)
			}
			// Make the store, backed by tmp dir if store location isn't provided.
			var s bundlestore.Store
			if *store == "" {
				*store = tmpDir.Dir
			}
			if !strings.HasPrefix(*store, "http://") {
				if s, err = bundlestore.MakeFileStore(&temp.TempDir{Dir: *store}); err != nil {
					return nil, err
				}
			} else {
				s = bundlestore.MakeHTTPStore(*store)
			}
			// Make the db and convert it into a filer.
			db := gitdb.MakeDBFromRepo(r, tmpDir, nil, nil, &gitdb.BundlestoreConfig{s}, gitdb.AutoUploadBundlestore)
			return snapshot.NewDBAdapter(db), nil
		},

		func() server.WorkerUri {
			return server.WorkerUri("http://" + *httpAddr)
		},

		func() execer.Memory {
			return execer.Memory(*memCapFlag)
		},
	)

	log.Println("Serving thrift on", *thriftAddr)
	server.RunServer(bag, schema, configText)
}
