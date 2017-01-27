package gitdb

type module struct{}

func Module() ice.Module {
	return module{}
}

func (m module) InstallModule(b *ice.MagicBag) {
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
	)
}

type NewRepoIniter struct {
	tmp *temp.TempDir
}

func (i *NewRepoIniter) Init() (*repo.Repository, error) {
	repoTmp, err := tmp.TempDir("gitdb-repo-")
	if err != nil {
		return nil, err
	}

	r, err := repo.InitRepo(repoTmp.Dir)
	if err != nil {
		return nil, err
	}
}
