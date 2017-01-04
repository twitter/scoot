package gitdb

import (
	"fmt"
	"strings"

	"github.com/scootdev/scoot/snapshot"
)

const localIDText = "local"
const localIDFmt = "%s-%s-%s"

// localValue holds a reference to a value that is in the local DB
type localValue struct {
	sha  string
	kind valueKind
}

const (
	snapshotIDText            = "snap"
	snapshotWithHistoryIDText = "swh"
)

var kindToIDText = map[valueKind]string{
	kindSnapshot:            snapshotIDText,
	kindSnapshotWithHistory: snapshotWithHistoryIDText,
}

var kindIDTextToKind = map[string]valueKind{
	snapshotIDText:            kindSnapshot,
	snapshotWithHistoryIDText: kindSnapshotWithHistory,
}

func (v *localValue) ID() snapshot.ID {
	return snapshot.ID(fmt.Sprintf(localIDFmt, localIDText, kindToIDText[v.kind], v.sha))
}

// parseID parses ID into a localValue
func parseID(id snapshot.ID) (*localValue, error) {
	if id == "" {
		return nil, fmt.Errorf("empty snapshot ID")
	}
	if !strings.HasPrefix(string(id), localIDText+"-") {
		return nil, fmt.Errorf("unrecognized snapshot ID scheme in %s", id)
	}
	parts := strings.Split(string(id), "-")
	if len(parts) != 3 {
		return nil, fmt.Errorf("did not scan 3 items from %s", id)
	}
	kindText, sha := parts[1], parts[2] // skip parts[0] because we know it's local

	kind, ok := kindIDTextToKind[kindText]
	if !ok {
		return nil, fmt.Errorf("invalid kind: %s", kindText)
	}

	if err := validSha(sha); err != nil {
		return nil, err
	}

	return &localValue{kind: kind, sha: sha}, nil
}

// validSha checks if sha is valid
func validSha(sha string) error {
	if len(sha) != 40 {
		return fmt.Errorf("sha not 40 characters: %s", sha)
	}
	// TODO(dbentley): check that it's hexadecimal?
	return nil
}
