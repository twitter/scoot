package gitdb

import (
	"fmt"
	"strings"

	"github.com/scootdev/scoot/snapshot"
)

// backend allows getting a snap for an ID, which can then be used to download the ID
type backend interface {
	parseID(id string, kind string, parts []string) (snap, error)
}

// upload allow uploading the ID. Note: The ID to upload will often be in another backend.
// E.g., for the git tags uploader, it will often pass a local snapshot
type uploader interface {
	upload(id snapshot.ID) (snapshot.ID, error)
	backend
}

// GitDB uses several different Backends to store Snapshots.
// snap is the common interface for a Snapshot (identified by an ID)
type snap interface {
	ID() snapshot.ID

	Kind() snapKind
	SHA() string
	Download(db *DB) error
}

// parseID parses ID into a snap
func (db *DB) parseID(id snapshot.ID) (snap, error) {
	if id == "" {
		return nil, fmt.Errorf("empty snapshot ID")
	}
	parts := strings.Split(string(id), "-")
	if len(parts) < 3 {
		return nil, fmt.Errorf("could not determine backend and kind in ID %s", id)
	}

	backendType, kind := parts[0], snapKind(parts[1])

	if !kinds[kind] {
		return nil, fmt.Errorf("invalid kind: %s", kind)
	}

	switch backendType {
	case localIDText:
		return db.local.parseID(id, kind, parts[2:])
	case streamIDText:
		return db.stream.parseID(id, kind, parts[2:])
	default:
		return nil, fmt.Errorf("unrecognized snapshot backend %s in ID %s", backendType, id)
	}
}

// validSha checks if sha is valid
func validSha(sha string) error {
	if len(sha) != 40 {
		return fmt.Errorf("sha not 40 characters: %s", sha)
	}
	// TODO(dbentley): check that it's hexadecimal?
	return nil
}
