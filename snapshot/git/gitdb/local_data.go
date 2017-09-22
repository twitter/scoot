package gitdb

import (
	"fmt"

	snap "github.com/twitter/scoot/snapshot"
)

const localIDText = "local"
const localIDFmt = "%s-%s-%s"

type localBackend struct{}

// localSnapshot holds a reference to a value that is in the local DB
type localSnapshot struct {
	sha  string
	kind SnapshotKind
}

func (b *localBackend) parseID(id snap.ID, kind SnapshotKind, extraParts []string) (*localSnapshot, error) {
	if len(extraParts) != 1 {
		return nil, fmt.Errorf("cannot parse snapshot ID: expected 3 extraParts in local id %s", id)
	}
	sha := extraParts[0]
	if err := validSha(sha); err != nil {
		return nil, err
	}

	return &localSnapshot{kind: kind, sha: sha}, nil
}

func (s *localSnapshot) ID() snap.ID {
	return snap.ID(fmt.Sprintf(localIDFmt, localIDText, s.kind, s.sha))
}
func (s *localSnapshot) Kind() SnapshotKind { return s.kind }
func (s *localSnapshot) SHA() string        { return s.sha }

func (s *localSnapshot) Download(db *DB) error {
	// a localSnapshot is either present already or we have no way to download it
	return db.shaPresent(s.SHA())
}
