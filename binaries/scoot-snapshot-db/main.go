package main

import (
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/scootapi"
	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/bundlestore"
	"github.com/scootdev/scoot/snapshot/cli"
	"github.com/scootdev/scoot/snapshot/git/gitdb"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

func main() {
	inj := &injector{}
	cmd := cli.MakeDBCLI(inj)
	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

type injector struct{}

func (i *injector) RegisterFlags(rootCmd *cobra.Command) {}

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

	_, api := scootapi.GetScootapiAddr()
	store, err := bundlestore.MakeHTTPStore(scootapi.APIAddrToBundlestoreURI(api)), nil
	if err != nil {
		return nil, err
	}

	return gitdb.MakeDBFromRepo(dataRepo, tempDir, nil, nil, &gitdb.BundlestoreConfig{Store: store},
		gitdb.AutoUploadBundlestore), nil
}
