package bundlestore

import (
	"github.com/scootdev/scoot/ice"
)

func Module() ice.Module {
	return module{}
}

type module struct{}

func (m module) Install(b *ice.MagicBag) {
	b.Put(MakeFileStoreInTemp)
	b.Put(MakeServer)
}
