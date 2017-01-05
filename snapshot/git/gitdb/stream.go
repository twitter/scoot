package gitdb

import (
	"fmt"
	"strings"

	"github.com/scootdev/scoot/snapshot"
)

// A Stream is a sequence of SnapshotWithHistory's.
type StreamConfig struct {
	Name    string
	Remote  string
	RefSpec string
}

const streamIDText = "stream"
const streamIDFmt = "%s-%s-%s"

func parseStreamID(id snapshot.ID) (*streamValue, error) {
	parts := strings.Split(string(id), "-")
	if len(parts) != 3 {
		return nil, fmt.Errorf("cannot parse snapshot ID: expected 2 hyphens in local id %s", id)
	}
	scheme, streamName, sha := parts[0], parts[1], parts[2]
	if scheme != streamIDText {
		return nil, fmt.Errorf("scheme not stream: %s", id)
	}

	if err := validSha(sha); err != nil {
		return nil, err
	}

	return &streamValue{streamName: streamName, sha: sha}, nil
}

func (db *DB) downloadStreamValue(v *streamValue) (snapshot.ID, error) {
	if db.stream == nil {
		return "", fmt.Errorf("cannot download snapshot %s: no streams configured", v.ID())
	}
	if v.streamName != db.stream.Name {
		return "", fmt.Errorf("cannot download snapshot %s: does not match stream %s", v.ID(), db.stream.Name)
	}

	_, err := db.dataRepo.Run("fetch", db.stream.Remote)
	return v.ID(), nil
}

type streamValue struct {
	streamName string
	sha        string
}

func (v *streamValue) ID() snapshot.ID {
	return snapshot.ID(fmt.Sprintf(streamIDFmt, streamIDText, v.streamName, v.sha))
}
