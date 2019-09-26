package bazel

import (
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/common/ice"
	"github.com/twitter/scoot/common/os/temp"
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
