package gitdb

import (
	"io/ioutil"
	"time"

	"github.com/twitter/scoot/ice"
	snap "github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/git/repo"
	"github.com/twitter/scoot/snapshot/store"
)

type module struct{}

// Module returns a module that supports typical GitDB usage
func Module() ice.Module {
	return module{}
}

// Install installs setup functions to use GitDB
func (m module) Install(b *ice.MagicBag) {
	b.PutMany(
		func() RepoIniter {
			return &TmpRepoIniter{}
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
		func() string {
			return ""
		},
		MakeDBNewRepo,
	)
}

// Noop/Tmp/Default implementations of gitdb RepoIniter/RepoUpdater interfaces

// TmpRepoIniter creates a new Repo in a temp dir
type TmpRepoIniter struct {
	tmp string
}

// Init creates a new temp dir and a repo in it
func (i *TmpRepoIniter) Init() (*repo.Repository, error) {
	repoTmp, err := ioutil.TempDir(i.tmp, "gitdb-repo-")
	if err != nil {
		return nil, err
	}

	return repo.InitRepo(repoTmp)
}

type NoopRepoUpdater struct{}

func (u *NoopRepoUpdater) Update(*repo.Repository) error { return nil }

func (u *NoopRepoUpdater) UpdateInterval() time.Duration { return snap.NoDuration }
