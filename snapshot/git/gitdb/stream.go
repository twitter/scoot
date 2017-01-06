package gitdb

import (
	"fmt"

	"github.com/scootdev/scoot/snapshot"
)

// A Stream is a sequence of SnapshotWithHistory's that updates via a
// Git ref that can be fetched from a Git remote
type StreamConfig struct {
	// Name (used in IDs, e.g. sm)
	Name string

	// Remote to fetch from (e.g. https://github.com/scootdev/scoot)
	Remote string

	// Name of ref to follow in data repo (e.g. refs/remotes/upstream/master)
	RefSpec string
}

const streamIDText = "stream"
const streamIDFmt = "%s-%s-%s-%s"

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

func (v *streamValue) id() snapshot.ID {
	return snapshot.ID(fmt.Sprintf(streamIDFmt, streamIDText, v.kind, v.streamName, v.sha))
}
func (v *streamValue) kindF() valueKind { return v.kind }
func (v *streamValue) shaF() string     { return v.sha }

func (v *streamValue) upload(db *DB) (snapshot.ID, error) {
	// A stream value is already present Globally
	return v.id(), nil
}

func (v *streamValue) download(db *DB) (snapshot.ID, error) {
	// TODO(dbentley): check if present before downloading

	if db.stream == nil {
		return "", fmt.Errorf("cannot download snapshot %s: no streams configured", v.id())
	}
	if v.streamName != db.stream.Name {
		return "", fmt.Errorf("cannot download snapshot %s: does not match stream %s", v.id(), db.stream.Name)
	}

	if _, err := db.dataRepo.Run("fetch", db.stream.Remote); err != nil {
		return "", err
	}
	return v.id(), nil
}
