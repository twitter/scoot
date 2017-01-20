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

	return &tagsSnap{kind: kind, sha: sha, remoteName: remoteName}, nil
}

func (b *tagsBackend) upload(s snapshot, db *DB) (snapshot, error) {
	switch s := s.(type) {
	case *tagsSnap:
		return s, nil
	case *streamSnap:
		return s, nil
	case *localSnap:
		if _, err := db.dataRepo.Run("tag", fmt.Sprintf("%s/%s", b.cfg.Prefix, s.SHA()), s.SHA()); err != nil {
			return nil, err
		}

		refSpec := fmt.Sprintf("%s/%s", b.cfg.Prefix, s.SHA())
		if _, err := db.dataRepo.Run("push", b.cfg.Remote, refSpec); err != nil {
			return nil, err
		}
		return &tagsSnap{s.SHA(), s.Kind(), b.cfg.Remote}, nil
	default:
		return nil, fmt.Errorf("cannot upload %v: unknown type %T", s, s)
	}

}

type tagsSnap struct {
	sha        string
	kind       snapshotKind
	remoteName string
}

func (s *tagsSnap) ID() snap.ID {
	return snap.ID(strings.Join([]string{tagsIDText, string(s.kind), s.remoteName, s.sha}, "-"))
}
func (s *tagsSnap) Kind() snapshotKind { return s.kind }
func (s *tagsSnap) SHA() string        { return s.sha }

func (s *tagsSnap) Download(db *DB) error {
	if err := db.shaPresent(s.SHA()); err == nil {
		return nil
	}

	// TODO(dbentley): keep stats about tag fetching (when we do it, last time we did it, etc.)

	refSpec := fmt.Sprintf("%s/%s", db.tags.cfg.Prefix, s.SHA())
	if _, err := db.dataRepo.Run("fetch", db.tags.cfg.Remote, refSpec); err != nil {
		return err
	}

	return db.shaPresent(s.SHA())
}
