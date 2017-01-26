package snapshots

import (
	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/snapshot"
)

func ViewModule() ice.Module {
	return module{}
}

type module struct{}

func (m module) Install(b *ice.MagicBag) {
	b.Put(NewViewServer)
}

func NewViewServer(db snapshot.DB) *ViewServer {
	return &ViewServer{db}
}
