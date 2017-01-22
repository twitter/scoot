package gitdb

import (
	snap "github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/bundlestore"
)

type bundlestoreBackend struct {
	store bundlestore.Store
}

const bundlestoreIDText = "bs"

// "bs-gc-<bundle>-<sha>"

func (b *bundlestoreBackend) parseID(id snap.ID, kind snapshotKind, extraParts []string) (snapshot, error) {
	if len(extraParts) != 2 {
		return nil, fmt.Errorf("cannot parse snapshot ID: expected 4 parts in bundlestore id: %s", id)
	}
	bundleName, sha := extraParts[0], extraParts[1]

	if err := validSha(sha); err != nil {
		return nil, err
	}

	return &bundlestoreSnapshot{kind: kind, sha: sha, bundleName: bundleName}, nil
}

func (b *tagsBackend) upload(s snapshot, db *DB) (snapshot, error) {
	// We only have to upload a localSnapshot
	switch s := s.(type) {
	case *tagsSnapshot:
		return s, nil
	case *streamSnapshot:
		return s, nil
	case *bundlestoreSnapshot:
		return s, nil
	case *localSnapshot:
		return &bundlestoreSnapshot{s.SHA(), s.Kind(), b.cfg.Name}, nil
	default:
		return nil, fmt.Errorf("cannot upload %v: unknown type %T", s, s)
	}
}

type bundlestoreSnapshot struct {
	sha        string
	kind       snapshotKind
	bundleName string
}

func (s *bundlestoreSnapshot) ID() snap.ID {
	return snap.ID(strings.Join([]string{bundlestoreIDText, string(s.kind), s.bundleName, s.sha}, "-"))
}
func (s *bundlestoreSnapshot) Kind() snapshotKind { return s.kind }
func (s *bundlestoreSnapshot) SHA() string        { return s.sha }

func (s *bundlestoreSnapshot) Download(db *DB) error {
	if err := db.shaPresent(s.SHA()); err != nil {
		return nil
	}

	// TODO(dbentley): keep stats about bundlestore downloading
	// TODO(dbentley): keep stats about how long it takes to unbundle
}
