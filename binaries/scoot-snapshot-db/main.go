package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/wisechengyi/scoot/common/dialer"
	"github.com/wisechengyi/scoot/common/log/hooks"
	"github.com/wisechengyi/scoot/common/stats"
	"github.com/wisechengyi/scoot/scheduler/client"
	"github.com/wisechengyi/scoot/snapshot"
	"github.com/wisechengyi/scoot/snapshot/cli"
	"github.com/wisechengyi/scoot/snapshot/git/gitdb"
	"github.com/wisechengyi/scoot/snapshot/git/repo"
	"github.com/wisechengyi/scoot/snapshot/store"
)

var dbTempDir string = ""

func main() {
	log.AddHook(hooks.NewContextHook())

	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
	flag.Parse()

	level, err := log.ParseLevel(*logLevelFlag)
	if err != nil {
		log.Error(err)
		return
	}
	log.SetLevel(level)

	inj := &injector{}
	cmd := cli.MakeDBCLI(inj)
	err = cmd.Execute()
	os.RemoveAll(dbTempDir)
	if err != nil {
		log.Fatal(err)
	}
}

type injector struct {
	// URL to bundlestore server
	storeURL string
}

func (i *injector) RegisterFlags(rootCmd *cobra.Command) {
	rootCmd.PersistentFlags().StringVar(&i.storeURL, "bundlestore_url", "", "bundlestore URL")
}

func (i *injector) Inject() (snapshot.DB, error) {
	dbTempDir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, err
	}

	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	dataRepo, err := repo.NewRepository(wd)
	if err != nil {
		return nil, fmt.Errorf(
			"cannot create a repo in wd %v; scoot-snapshot-db must be run in a git repo: %v", wd, err)
	}

	resolver := dialer.NewCompositeResolver(
		dialer.NewConstantResolver(i.storeURL),
		dialer.NewEnvResolver("SCOOT_BUNDLESTORE_URL"),
		client.NewBundlestoreResolver())
	url, err := resolver.Resolve()
	if err != nil {
		return nil, err
	}

	store := store.MakeHTTPStore(url)
	return gitdb.MakeDBFromRepo(
			dataRepo, nil, dbTempDir, nil, nil,
			&gitdb.BundlestoreConfig{Store: store},
			gitdb.AutoUploadBundlestore,
			stats.NilStatsReceiver()),
		nil
}
