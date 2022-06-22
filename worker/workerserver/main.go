package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/cloud/cluster/local"
	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/runners"
	"github.com/twitter/scoot/snapshot/git/gitdb"
	"github.com/twitter/scoot/snapshot/store"
	"github.com/twitter/scoot/worker/client"
	"github.com/twitter/scoot/worker/domain"
	"github.com/twitter/scoot/worker/starter"
)

func main() {
	log.AddHook(hooks.NewContextHook())

	thriftAddr := flag.String("thrift_addr", domain.DefaultWorker_Thrift, "addr to serve thrift on")
	httpAddr := flag.String("http_addr", domain.DefaultWorker_HTTP, "addr to serve http on")
	memCapFlag := flag.Uint64("mem_cap", 0, "Kill runs that exceed this amount of memory, in bytes. Zero means no limit.")
	storeHandle := flag.String("bundlestore", "", "Abs file path or an http 'host:port' to store/get bundles.")
	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
	flag.Parse()

	level, err := log.ParseLevel(*logLevelFlag)
	if err != nil {
		log.Error(err)
		return
	}
	log.SetLevel(level)

	stat := starter.GetStatsReceiver()

	store, err := getStore(*storeHandle)
	if err != nil {
		log.Fatal(err)
	}
	db := gitdb.MakeDBNewRepo(
		&gitdb.TmpRepoIniter{},
		&gitdb.NoopRepoUpdater{},
		"",
		nil,
		nil,
		&gitdb.BundlestoreConfig{Store: store, AllowStreamUpdate: true},
		gitdb.AutoUploadBundlestore,
		stat)
	oc, err := runners.NewHttpOutputCreator(("http://" + *httpAddr + "/output/"))
	if err != nil {
		log.Fatal(err)
	}
	starter.StartServer(
		*thriftAddr,
		*httpAddr,
		db,
		oc,
		getRunnerID(),
		stats.NopDirsMonitor,
		*memCapFlag,
		&stat,
		[]func() error{},
		[]func() error{},
		nil,
		nil,
	)
}

// Use storeHandle if provided, else try Fetching, then GetScootApiAddr(), then fallback to tmp file store.
func getStore(storeHandle string) (store.Store, error) {
	if storeHandle != "" {
		if strings.HasPrefix(storeHandle, "/") {
			return store.MakeFileStoreInTemp()
		} else {
			return store.MakeHTTPStore(client.APIAddrToBundlestoreURI(storeHandle)), nil
		}
	}
	storeAddr := ""
	nodes, _ := local.MakeFetcher("apiserver", "http_addr").Fetch()
	if len(nodes) > 0 {
		r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
		storeAddr = string(nodes[r.Intn(len(nodes))].Id())
		log.Info("No stores specified, but successfully fetched store addr: ", nodes, " --> ", storeAddr)
	} else {
		_, storeAddr, _ = client.GetScootapiAddr()
		log.Info("No stores specified, but successfully read .cloudscootaddr: ", storeAddr)
	}
	if storeAddr != "" {
		return store.MakeHTTPStore(client.APIAddrToBundlestoreURI(storeAddr)), nil
	}
	log.Info("No stores specified or found, creating a tmp file store")
	return store.MakeFileStoreInTemp()
}

func getRunnerID() runner.RunnerID {
	// suitable local testing purposes, but a production implementation would supply a unique ID
	hostname, _ := os.Hostname()
	hostname = fmt.Sprintf("%s-%d", hostname, rand.Intn(10000))
	return runner.RunnerID{ID: hostname}
}
