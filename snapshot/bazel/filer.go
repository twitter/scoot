package bazel

import (
	"fmt"

	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/snapshots"
)

func MakeBzFiler(tmp *temp.TempDir, r dialer.Resolver) (*BzFiler, error) {
	return makeBzFiler(tmp, r, false)
}

func MakeBzFilerKeepCheckouts(tmp *temp.TempDir, r dialer.Resolver) (*BzFiler, error) {
	return makeBzFiler(tmp, r, true)
}

func makeBzFiler(tmp *temp.TempDir, r dialer.Resolver, keep bool) (*BzFiler, error) {
	if tmp == nil {
		return nil, fmt.Errorf("TempDir must be provided to MakeBzFiler")
	}
	treeDir, err := tmp.TempDir("bztree")
	if err != nil {
		return nil, err
	}

	bf := &BzFiler{
		tree:          makeBzCommand(treeDir.Dir, r),
		tmp:           tmp,
		keepCheckouts: keep,
		CASResolver:   r,
		updater:       snapshots.MakeNoopUpdater(),
	}
	return bf, nil
}

// Implements Snapshot interface (snapshot.Checkouter, snapshot.Ingester, and snapshot.Updater)
// Default tree implementation uses fs_util, a tool provided by pants at
// https://github.com/pantsbuild/binaries/tree/gh-pages/build-support/bin/fs_util
// which handles underlying implementation of bazel snapshot functionality
type BzFiler struct {
	tree bzTree
	tmp  *temp.TempDir

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
	updater     snapshot.Updater
}

// Interface that specifies actions on directory tree structures for Bazel
type bzTree interface {
	save(path string) (string, error)  // Save a directory glob, return a SnapshotID as string
	materialize(sha, dir string) error // Unpack a tree from a sha into target directory
}
