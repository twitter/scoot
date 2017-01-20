package gitdb

import (
	"fmt"
	"strings"

	snap "github.com/scootdev/scoot/snapshot"
)

type TagsConfig struct {
	Name   string
	Remote string
	Prefix string
}

type tagsBackend struct {
	cfg *TagsConfig
}

const tagsIDText = "tags"

// tags-sss-<sha>

func (b *tagsBackend) parseID(id snap.ID, kind snapshotKind, parts []string) (snapshot, error) {
	if len(parts) != 2 {
		return nil, fmt.Errorf("cannot parse snapshot ID: expected 4 parts in tags id: %s", id)
	}
	remoteName, sha := parts[0], parts[1]

	if err := validSha(sha); err != nil {
		return nil, err
	}

	return &tagsSnapshot{kind: kind, sha: sha, remoteName: remoteName}, nil
}

func (b *tagsBackend) upload(s snapshot, db *DB) (snapshot, error) {
	switch s := s.(type) {
	case *tagsSnapshot:
		return s, nil
	case *streamSnapshot:
		return s, nil
	case *localSnapshot:
		tag := makeTag(b.cfg.Prefix, s.SHA())
		if _, err := db.dataRepo.Run("tag", tag, s.SHA()); err != nil {
			return nil, err
		}

		if _, err := db.dataRepo.Run("push", b.cfg.Remote, tag); err != nil {
			return nil, err
		}
		return &tagsSnapshot{s.SHA(), s.Kind(), b.cfg.Remote}, nil
	default:
		return nil, fmt.Errorf("cannot upload %v: unknown type %T", s, s)
	}

}

type tagsSnapshot struct {
	sha        string
	kind       snapshotKind
	remoteName string
}

func (s *tagsSnapshot) ID() snap.ID {
	return snap.ID(strings.Join([]string{tagsIDText, string(s.kind), s.remoteName, s.sha}, "-"))
}
func (s *tagsSnapshot) Kind() snapshotKind { return s.kind }
func (s *tagsSnapshot) SHA() string        { return s.sha }

func (s *tagsSnapshot) Download(db *DB) error {
	if err := db.shaPresent(s.SHA()); err == nil {
		return nil
	}

	// TODO(dbentley): keep stats about tag fetching (when we do it, last time we did it, etc.)

	if s.remoteName != db.tags.cfg.Remote {
		return fmt.Errorf("cannot download %v: remote %s is not registered (expected %v)", s.ID(), s.remoteName, db.tags.cfg.Remote)
	}

	if _, err := db.dataRepo.Run("fetch", db.tags.cfg.Remote, makeTag(db.tags.cfg.Prefix, s.SHA())); err != nil {
		return err
	}

	return db.shaPresent(s.SHA())
}

func makeTag(prefix string, sha string) string {
	return fmt.Sprintf("%s/%s", prefix, sha)
}
