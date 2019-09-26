package gitdb

import (
	"time"

	snap "github.com/twitter/scoot/apiserver/snapshot"
	"github.com/twitter/scoot/apiserver/snapshot/git/repo"
	"github.com/twitter/scoot/apiserver/snapshot/store"
	"github.com/twitter/scoot/common/ice"
	"github.com/twitter/scoot/common/os/temp"
)

type module struct{}

// Module returns a module that supports typical GitDB usage
func Module() ice.Module {
	return module{}
}

// Install installs setup functions to use GitDB
func (m module) Install(b *ice.MagicBag) {
	b.PutMany(
		func(tmp *temp.TempDir) RepoIniter {
			return &TmpRepoIniter{tmp: tmp}
		},
		func() RepoUpdater {
			return &NoopRepoUpdater{}
		},
		func(store store.Store) *BundlestoreConfig {
			return &BundlestoreConfig{Store: store, AllowStreamUpdate: true}
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
	)
}

// Noop/Tmp/Default implementations of gitdb RepoIniter/RepoUpdater interfaces

// TmpRepoIniter creates a new Repo in a temp dir
type TmpRepoIniter struct {
	tmp *temp.TempDir
}

// Init creates a new temp dir and a repo in it
func (i *TmpRepoIniter) Init() (*repo.Repository, error) {
	repoTmp, err := i.tmp.TempDir("gitdb-repo-")
	if err != nil {
		return nil, err
	}

	return repo.InitRepo(repoTmp.Dir)
}

type NoopRepoUpdater struct{}

func (u *NoopRepoUpdater) Update(*repo.Repository) error { return nil }

func (u *NoopRepoUpdater) UpdateInterval() time.Duration { return snap.NoDuration }
