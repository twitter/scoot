package main

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"os"

	"github.com/spf13/cobra"

	"github.com/scootdev/scoot/common/dialer"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/scootapi"
	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/bundlestore"
	"github.com/scootdev/scoot/snapshot/cli"
	"github.com/scootdev/scoot/snapshot/git/gitdb"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

func main() {
	// log.SetFlags(log.LstdFlags | log.LUTC | log.Lshortfile)

	inj := &injector{}
	cmd := cli.MakeDBCLI(inj)
	if err := cmd.Execute(); err != nil {
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
	tempDir, err := temp.TempDirDefault()
	if err != nil {
		return nil, err
	}
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
	return gitdb.MakeDBFromRepo(dataRepo, tempDir, nil, nil, &gitdb.BundlestoreConfig{Store: store},
		gitdb.AutoUploadBundlestore), nil
}
