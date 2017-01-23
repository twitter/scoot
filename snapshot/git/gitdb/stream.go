package gitdb

import (
	"fmt"

	snap "github.com/scootdev/scoot/snapshot"
)

// A Stream is a sequence of GitCommitSnapshots that updates.
// Right now, the backend is a git refspec that can be fetched from a Git remote.
// In a v2, we'd rather not talk to a git server at all (because fetch can be confusingly slow).
// in that case, a stream would need to be a mutable and consistent pointer to a GitCommitSnapshot.
// Instead of using an immutable key-value store for (large) bundles, we'd back streams with
// a small, mutable key-value store that points to the large, immutable key-value store for Snapshots.
type StreamConfig struct {
	// Name (used in IDs (so it should be short)
	// e.g. sm for a Stream following Source (repo)'s Master (branch)
	Name string

	// Remote to fetch from (e.g. https://github.com/scootdev/scoot)
	Remote string

	// Name of ref to follow in data repo (e.g. refs/remotes/upstream/master)
	RefSpec string
}

const streamIDText = "stream"
const streamIDFmt = "%s-%s-%s-%s"

type streamBackend struct {
	cfg *StreamConfig
}

func (b *streamBackend) parseID(id snap.ID, kind snapshotKind, extraParts []string) (*streamSnapshot, error) {
	if len(extraParts) != 2 {
		return nil, fmt.Errorf("cannot parse snapshot ID: expected 4 extraParts in stream id: %s", id)
	}
	streamName, sha := extraParts[0], extraParts[1]

	if err := validSha(sha); err != nil {
		return nil, err
	}

	return &streamSnapshot{streamName: streamName, kind: kind, sha: sha}, nil
}

// streamSnapshot represents a Snapshot that lives in a Stream
type streamSnapshot struct {
	sha        string
	kind       snapshotKind
	streamName string
}

func (s *streamSnapshot) ID() snap.ID {
	return snap.ID(fmt.Sprintf(streamIDFmt, streamIDText, s.kind, s.streamName, s.sha))
}
func (s *streamSnapshot) Kind() snapshotKind { return s.kind }
func (s *streamSnapshot) SHA() string        { return s.sha }

func (s *streamSnapshot) Download(db *DB) error {
	if err := db.shaPresent(s.SHA()); err == nil {
		// Already present!
		return nil
	}

	// TODO(dbentley): what if we've already fetched recently? We should figure out some way to
	// prevent that

	if db.stream == nil {
		return fmt.Errorf("cannot download snapshot %s: no streams configured", s.ID())
	}

	if err := db.stream.updateStream(s.streamName, db); err != nil {
		return err
	}

	return db.shaPresent(s.SHA())
}

// updateStream updates the named stream
func (b *streamBackend) updateStream(name string, db *DB) error {
	if name != b.cfg.Name {
		return fmt.Errorf("cannot update stream %s: does not match stream %s", name, db.stream.cfg.Name)
	}

	// TODO(dbentley): keep stats about fetching (when we do it, last time we did it, etc.)
	_, err := db.dataRepo.Run("fetch", b.cfg.Remote)
	return err
}
