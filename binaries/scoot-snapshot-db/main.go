package main

import (
	"flag"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/scootapi"
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/bundlestore"
	"github.com/twitter/scoot/snapshot/cli"
	"github.com/twitter/scoot/snapshot/git/gitdb"
	"github.com/twitter/scoot/snapshot/git/repo"
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
	if err := cmd.Execute(); err != nil {
		removeTemp()
		log.Fatal(err)
	}
	removeTemp()
}

type injector struct {
	// URL to bundlestore server
	storeURL string
}

func (i *injector) RegisterFlags(rootCmd *cobra.Command) {
	rootCmd.PersistentFlags().StringVar(&i.storeURL, "bundlestore_url", "", "bundlestore URL")
}

func (i *injector) Inject() (snapshot.DB, error) {
	tempDir, err := temp.TempDirDefault()
	if err != nil {
		return nil, err
	}
	dbTempDir = tempDir.Dir
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	// So... this is cheating, a bit. Because it means that we have a dataRepo
	// that we don't own. We trust that we're running in a repo that's valid,
	// has the right remotes, etc.
	// We need this because we can't have every invocation of scoot-snapshot-db
	// create a new repo.
	// Eventually, we would love to talk to the Daemon, which could have its own DB
	// with its own repo, that it maintains.
	dataRepo, err := repo.NewRepository(wd)
	if err != nil {
		return nil, fmt.Errorf(
			"cannot create a repo in wd %v; scoot-snapshot-db must be run in a git repo: %v", wd, err)
	}

	resolver := dialer.NewCompositeResolver(
		dialer.NewConstantResolver(i.storeURL),
		dialer.NewEnvResolver("SCOOT_BUNDLESTORE_URL"),
		scootapi.NewBundlestoreResolver())
	url, err := resolver.Resolve()
	if err != nil {
		return nil, err
	}

	store := bundlestore.MakeHTTPStore(url)
	return gitdb.MakeDBFromRepo(
			dataRepo, nil, tempDir, nil, nil,
			&gitdb.BundlestoreConfig{Store: store},
			gitdb.AutoUploadBundlestore,
			stats.NilStatsReceiver()),
		nil
}

func removeTemp() {
	if dbTempDir != "" {
		os.RemoveAll(dbTempDir)
	}
}
