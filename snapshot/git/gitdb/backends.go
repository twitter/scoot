package gitdb

import (
	"fmt"
	"strings"

	snap "github.com/twitter/scoot/snapshot"
)

// backend allows getting a snapshot for an ID, which can then be used to download the ID
type backend interface {
	parseID(id snap.ID, kind SnapshotKind, extraParts []string) (snapshot, error)
}

// upload allow uploading the ID. Note: The ID to upload will often be in another backend.
// E.g., for the git tags uploader, it will often pass a local snapshot
type uploader interface {
	upload(s snapshot, db *DB) (snapshot, error)
	backend
}

// GitDB uses several different Backends to store Snapshots.
// snapshot is the common interface for a Snapshot (identified by an ID)
type snapshot interface {
	ID() snap.ID

	Kind() SnapshotKind
	SHA() string
	Download(db *DB) error
}

// parseID parses ID into a snapshot
func (db *DB) parseID(id snap.ID) (snapshot, error) {
	if id == "" {
		return nil, fmt.Errorf("empty snapshot ID")
	}
	parts := strings.Split(string(id), "-")
	if len(parts) < 3 {
		return nil, fmt.Errorf("could not determine backend and kind in ID %s", id)
	}

	backendType, kind := parts[0], SnapshotKind(parts[1])

	if !kinds[kind] {
		return nil, fmt.Errorf("invalid kind: %s", kind)
	}

	switch backendType {
	case localIDText:
		return db.local.parseID(id, kind, parts[2:])
	case streamIDText:
		return db.stream.parseID(id, kind, parts[2:])
	case tagsIDText:
		return db.tags.parseID(id, kind, parts[2:])
	case bundlestoreIDText:
		return db.bundles.parseID(id, kind, parts[2:])
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
