package gitdb

import (
	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/os/temp"
	snap "github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/bundlestore"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

type module struct{}

func Module() ice.Module {
	return module{}
}

func (m module) Install(b *ice.MagicBag) {
	b.PutMany(
		func(tmp *temp.TempDir) RepoIniter {
			return &NewRepoIniter{tmp: tmp}
		},
		func(store bundlestore.Store) *BundlestoreConfig {
			return &BundlestoreConfig{Store: store}
		},
		func() *StreamConfig {
			return nil
		},
		func() *TagsConfig {
			return nil
		},
		func() AutoUploadDest {
			return AutoUploadBundlestore
		},
		MakeDBNewRepo,
		func(db *DB) snap.DB { return db },
	)
}

type NewRepoIniter struct {
	tmp *temp.TempDir
}

func (i *NewRepoIniter) Init() (*repo.Repository, error) {
	repoTmp, err := i.tmp.TempDir("gitdb-repo-")
	if err != nil {
		return nil, err
	}

	return repo.InitRepo(repoTmp.Dir)
}
