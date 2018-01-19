package bazel

import (
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/snapshots"
)

func MakeBzFiler(r dialer.Resolver) *BzFiler {
	return &BzFiler{
		command:     &bzCommand{},
		updater:     snapshots.MakeNoopUpdater(),
		CASResolver: r,
	}
}

// Options is a variadic list of functions that take a *bzCommand as an arg
// and modify its fields, e.g.
// localStorePath := func(bc *bzCommand) {
//     bc.localStorePath = "/path/to/local/store"
// }
func MakeBzFilerWithOptions(r dialer.Resolver, options ...func(*bzCommand)) *BzFiler {
	return &BzFiler{
		updater:     snapshots.MakeNoopUpdater(),
		command:     MakeBzCommandWithOptions(options...),
		CASResolver: r,
	}
}

func MakeBzFilerWithOptionsKeepCheckouts(r dialer.Resolver, options ...func(*bzCommand)) *BzFiler {
	return &BzFiler{
		updater:       snapshots.MakeNoopUpdater(),
		command:       MakeBzCommandWithOptions(options...),
		keepCheckouts: true,
		CASResolver:   r,
	}
}

// Satisfies snapshot.Checkouter, snapshot.Ingester, and snapshot.Updater
// Default command is fs_util, a tool provided by github.com/pantsbuild/pants which
// handles underlying implementation of bazel snapshot functionality
type BzFiler struct {
	command       bzRunner
	updater       snapshot.Updater
	keepCheckouts bool
	// keepCheckouts exists for debuggability. Instead of removing checkouts on release,
	// we can optionally keep them to inspect

	// Public resolver exposes selection of server host:port for underlying connections to cas
	// NOTE: we may want to introduce a custom resolver that limits the number of underlying
	// resolve calls if these are costly (i.e. make a network request) and we make a high number
	// of CAS requests.
	// GRPC package provides tools for making client connection contexts that support
	// retry and backoff configuration, but we currently have to expose the resolver to
	// an underlying tool that makes CAS requests on our behalf during Checkout and Ingest.
	CASResolver dialer.Resolver
}
