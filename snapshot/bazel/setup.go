package bazel

import (
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/ice"
	"github.com/twitter/scoot/os/temp"
)

type module struct{}

func Module() ice.Module {
	return module{}
}

func (m module) Install(b *ice.MagicBag) {
	b.PutMany(
		func(tmp *temp.TempDir) (*BzFiler, error) {
			return MakeBzFiler(tmp, dialer.NewConstantResolver(""))
		},
	)
}
