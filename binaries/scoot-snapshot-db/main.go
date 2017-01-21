package main

import (
	"log"
	"os"

	"github.com/spf13/cobra"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot"
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

	dataRepo, err := repo.NewRepository(wd)
	if err != nil {
		return nil, err
	}

	return gitdb.MakeDB(dataRepo, tempDir, nil), nil
}
