package snapshots

import (
	"github.com/wisechengyi/scoot/ice"
	"github.com/wisechengyi/scoot/snapshot"
	"github.com/wisechengyi/scoot/snapshot/git/gitdb"
	"github.com/wisechengyi/scoot/snapshot/store"
)

// Module returns a module to allow serving Snapshots as an http.Handler
func Module() ice.Module {
	return module{}
}

type module struct{}

// Install installs the functions to serve Snapshots over HTTP
func (m module) Install(b *ice.MagicBag) {
	b.PutMany(
		func(gitDB *gitdb.DB) snapshot.DB {
			return gitDB
		},
		NewViewServer,
		func() *store.TTLConfig {
			return &store.TTLConfig{TTL: store.DefaultTTL, TTLKey: store.DefaultTTLKey, TTLFormat: store.DefaultTTLFormat}
		},
	)
}
