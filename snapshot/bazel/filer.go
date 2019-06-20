package bazel

import (
	"fmt"

	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/snapshots"
)

func MakeBzFiler(tmp *temp.TempDir, r dialer.Resolver) (*BzFiler, error) {
	return makeBzFiler(tmp, r, nil, false)
}

func MakeBzFilerUpdater(tmp *temp.TempDir, r dialer.Resolver, u snapshot.Updater) (*BzFiler, error) {
	return makeBzFiler(tmp, r, u, false)
}

func MakeBzFilerKeepCheckouts(tmp *temp.TempDir, r dialer.Resolver) (*BzFiler, error) {
	return makeBzFiler(tmp, r, nil, true)
}

func MakeBzFilerUpdaterKeepCheckouts(tmp *temp.TempDir, r dialer.Resolver, u snapshot.Updater) (*BzFiler, error) {
	return makeBzFiler(tmp, r, u, true)
}

func makeBzFiler(tmp *temp.TempDir, r dialer.Resolver, u snapshot.Updater, keep bool) (*BzFiler, error) {
	if tmp == nil {
		return nil, fmt.Errorf("TempDir must be provided to MakeBzFiler")
	}
	treeDir, err := tmp.TempDir("bztree")
	if err != nil {
		return nil, err
	}
	if u == nil {
		u = snapshots.MakeNoopUpdater()
	}

	bf := &BzFiler{
		tree:          makeBzCommand(treeDir.Dir, r),
		tmp:           tmp,
		keepCheckouts: keep,
		CASResolver:   r,
		updater:       u,
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

	// keepCheckouts exists for debuggability. Instead of removing checkouts on release,
	// we can optionally keep them to inspect
	keepCheckouts bool

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
	save(path string) (string, error)                     // Save a directory glob, return a SnapshotID as string
	materialize(sha string, size int64, dir string) error // Unpack a tree from a sha into target directory
}
