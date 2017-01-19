package gitdb

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

func (b *tagsBackend) parseID(id snapshot.ID, kind snapKind, parts []string) (*tagsSnap, error) {
	if len(parts) != 2 {
		return "", fmt.Errorf("cannot parse snapshot ID: expected 4 parts in tags id: %s", id)
	}
	serverName, sha := parts[0], parts[1]

	if err := validSha(sha); err != nil {
		return nil, err
	}

	return &tagsSnap{serverName: serverName, kind: kind, sha: sha}, nil
}

type tagsSnap struct {
	sha        string
	kind       snapKind
	remoteName string
}

func (s *tagsSnap) ID() snapshot.ID {
	return snapshot.ID(strings.Join([]string{tagsIDText, s.kind, s.remoteName, s.sha}, "-"))
}
func (s *tagsSnap) Kind() snapKind { return s.kind }
func (s *tagsSnap) SHA() string    { return s.sha }

func (s *streamSnap) Download(db *DB) error {
	if err := db.shaPresent(s.SHA()); err == nil {
		return nil
	}

	// TODO(dbentley): keep stats about tag fetching (when we do it, last time we did it, etc.)

	refSpec := fmt.Sprintf("refs/tags/%s%s:refs/remotes/%s/%s%s",
		db.tags.cfg.Prefix, s.SHA(),
		db.tags.cfg.Remote, db.tags.cfg.Prefix, s.SHA())
	if _, err := db.dataRepo.Run("fetch", db.tags.cfg.Remote, refSpec); err != nil {
		return err
	}

	return db.shaPresent(s.SHA())
}
