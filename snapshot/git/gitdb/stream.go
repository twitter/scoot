package gitdb

import (
	"fmt"

	"github.com/scootdev/scoot/snapshot"
)

// A Stream is a sequence of SnapshotWithHistory's that updates via a
// Git ref that can be fetched from a Git remote
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

// parse id as a sream ID, with kind and remaining parts (after scheme and kind were parsed)
func parseStreamID(id snapshot.ID, kind valueKind, parts []string) (*streamValue, error) {
	streamName, sha := parts[0], parts[1]

	if err := validSha(sha); err != nil {
		return nil, err
	}

	return &streamValue{streamName: streamName, kind: kind, sha: sha}, nil
}

type streamValue struct {
	sha        string
	kind       valueKind
	streamName string
}

func (v *streamValue) ID() snapshot.ID {
	return snapshot.ID(fmt.Sprintf(streamIDFmt, streamIDText, v.kind, v.streamName, v.sha))
}
func (v *streamValue) Kind() valueKind { return v.kind }
func (v *streamValue) SHA() string     { return v.sha }

func (v *streamValue) Upload(db *DB) (snapshot.ID, error) {
	// A stream value is already present Globally
	return v.ID(), nil
}

func (v *streamValue) Download(db *DB) (snapshot.ID, error) {
	// TODO(dbentley): check if present before downloading

	// TODO(dbentley): what if we've already fetched recently? We should figure out some way to
	// prevent

	if db.stream == nil {
		return "", fmt.Errorf("cannot download snapshot %s: no streams configured", v.ID())
	}
	if v.streamName != db.stream.Name {
		return "", fmt.Errorf("cannot download snapshot %s: does not match stream %s", v.ID(), db.stream.Name)
	}

	// TODO(dbentley): keep stats about fetching (when we do it, last time we did it, etc.)
	if _, err := db.dataRepo.Run("fetch", db.stream.Remote); err != nil {
		return "", err
	}
	return v.ID(), nil
}
