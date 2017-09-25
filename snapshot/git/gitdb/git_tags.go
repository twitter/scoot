package gitdb

import (
	"errors"
	"fmt"
	"strings"

	snap "github.com/twitter/scoot/snapshot"
)

// TagsConfig specifies how GitDB should store Snapshots as tags in another git repo
type TagsConfig struct {
	// Name (used in IDs (so it should be short)
	// e.g. sss for Source (repo)'s Scoot Snapshots
	Name string

	// Remote to fetch from and push to (e.g. https://github.com/twitter/scoot)
	Remote string

	// Prefix for tag names (e.g. reserved_scoot)
	Prefix string
}

type tagsBackend struct {
	cfg *TagsConfig
}

const tagsIDText = "tags"

func (b *tagsBackend) parseID(id snap.ID, kind SnapshotKind, extraParts []string) (snapshot, error) {
	if b.cfg == nil {
		return nil, errors.New("Tags backend not initialized.")
	}

	if len(extraParts) != 2 {
		return nil, fmt.Errorf("cannot parse snapshot ID: expected 4 parts in tags id: %s", id)
	}
	name, sha := extraParts[0], extraParts[1]

	if err := validSha(sha); err != nil {
		return nil, err
	}

	return &tagsSnapshot{kind: kind, sha: sha, name: name}, nil
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
		// create the tag and then push it
		tag := makeTag(b.cfg.Prefix, s.SHA())
		if _, err := db.dataRepo.Run("tag", tag, s.SHA()); err != nil {
			return nil, err
		}

		if _, err := db.dataRepo.Run("push", b.cfg.Remote, tag); err != nil {
			return nil, err
		}
		return &tagsSnapshot{s.SHA(), s.Kind(), b.cfg.Name}, nil
	default:
		return nil, fmt.Errorf("cannot upload %v: unknown type %T", s, s)
	}
}

// tagsSnapshot represents a Snapshot that lives in a Tag
type tagsSnapshot struct {
	sha  string
	kind SnapshotKind
	name string
}

func (s *tagsSnapshot) ID() snap.ID {
	return snap.ID(strings.Join([]string{tagsIDText, string(s.kind), s.name, s.sha}, "-"))
}
func (s *tagsSnapshot) Kind() SnapshotKind { return s.kind }
func (s *tagsSnapshot) SHA() string        { return s.sha }

func (s *tagsSnapshot) Download(db *DB) error {
	if err := db.shaPresent(s.SHA()); err == nil {
		return nil
	}

	// TODO(dbentley): keep stats about tag fetching (when we do it, last time we did it, etc.)
	if s.name != db.tags.cfg.Name {
		return fmt.Errorf("cannot download %v: tags backend named %s is not registered (expected %v)", s.ID(), s.name, db.tags.cfg.Remote)
	}

	if _, err := db.dataRepo.Run("fetch", db.tags.cfg.Remote, makeTag(db.tags.cfg.Prefix, s.SHA())); err != nil {
		return err
	}

	return db.shaPresent(s.SHA())
}

// we store tags as <prefix>/<sha>, e.g. reserved_scoot/1cdbb0e2c889a2e301589cc349615c2c1545f641
func makeTag(prefix string, sha string) string {
	return fmt.Sprintf("%s/%s", prefix, sha)
}
