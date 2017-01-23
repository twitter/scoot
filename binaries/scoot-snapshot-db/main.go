package main

import (
	"fmt"
	"log"
	"os"

	"github.com/scootdev/scoot/os/temp"
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

// If 'storeDir' is nil don't use bundlestore backend, else use a file-backed bundlestore at that location.
func (i *injector) Inject(storeDir *temp.TempDir) (snapshot.DB, error) {
	tempDir, err := temp.TempDirDefault()
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

	var bundleConfig *gitdb.BundlestoreConfig
	upload := gitdb.AutoUploadNone
	if storeDir != nil {
		if s, err := bundlestore.MakeFileStore(storeDir); err != nil {
			return nil, err
		} else {
			bundleConfig = &gitdb.BundlestoreConfig{s}
			upload = gitdb.AutoUploadBundlestore
		}
	}

	return gitdb.MakeDBFromRepo(dataRepo, tempDir, nil, nil, bundleConfig, upload), nil
}
