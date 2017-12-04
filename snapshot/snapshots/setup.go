package snapshots

import (
	"github.com/twitter/scoot/ice"
	"github.com/twitter/scoot/snapshot/store"
)

// Module returns a module to allow serving Snapshots as an http.Handler
func Module() ice.Module {
	return module{}
}

type module struct{}

// Install installs the functions to serve Snapshots over HTTP
func (m module) Install(b *ice.MagicBag) {
	b.PutMany(
		NewViewServer,
		func() *store.TTLConfig {
			return &store.TTLConfig{TTL: store.DefaultTTL, TTLKey: store.DefaultTTLKey}
		},
	)
}
