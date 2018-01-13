package bazel

import (
	"github.com/twitter/scoot/ice"
)

type module struct{}

func Module() ice.Module {
	return module{}
}

func (m module) Install(b *ice.MagicBag) {
	b.PutMany(
		func() *BzFiler {
			return MakeBzFilerWithOptions()
		},
	)
}
