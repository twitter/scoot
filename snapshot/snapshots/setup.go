package snapshots

import (
	"github.com/scootdev/scoot/ice"
)

// Module returns a module to allow serving Snapshots
func Module() ice.Module {
	return module{}
}

type module struct{}

// Install installs the functions to serve Snapshots
func (m module) Install(b *ice.MagicBag) {
	b.Put(NewViewServer)
}
