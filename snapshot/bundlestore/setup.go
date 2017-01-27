package bundlestore

import (
	"github.com/scootdev/scoot/ice"
)

// Module returns a module that supports serving Bundlestore
func Module() ice.Module {
	return module{}
}

type module struct{}

// Install installs functions for serving Bundlestore
func (m module) Install(b *ice.MagicBag) {
	b.Put(MakeFileStoreInTemp)
	b.Put(MakeServer)
}
