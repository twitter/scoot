package gitdb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/wisechengyi/scoot/common/stats"
	snap "github.com/wisechengyi/scoot/snapshot"
	"github.com/wisechengyi/scoot/snapshot/git/repo"
)

// A Stream is a sequence of GitCommitSnapshots that updates.
// The backend is a git refspec that can be fetched from a Git remote.
type StreamConfig struct {
	// Name (used in IDs (so it should be short)
	// e.g. sm for a Stream following Source (repo)'s Master (branch)
	//      name may include a ref string that'll override the refspec configured for 'sm'
	//      the full id might look like "stream-gc-sm:<branch>-<sha>" for a given branch and sha
	Name string

	// Remote to fetch from (e.g. https://github.com/wisechengyi/scoot)
	Remote string

	// Name of ref to follow in data repo (e.g. refs/remotes/upstream/master)
	RefSpec string
}

const streamIDText = "stream"
const streamIDFmt = "%s-%s-%s-%s"
const streamNameShaSuffix = ":sha"

type streamBackend struct {
	cfg  *StreamConfig
	stat stats.StatsReceiver
}

func (b *streamBackend) parseID(id snap.ID, kind SnapshotKind, extraParts []string) (*streamSnapshot, error) {
	if b.cfg == nil {
		return nil, errors.New("Stream backend not initialized.")
	}

	if len(extraParts) < 2 {
		return nil, fmt.Errorf("cannot parse snapshot ID: expected 4 extraParts in stream id: %s", id)
	}
	// The last token is the sha, anything before that is a stream name that may include '-'.
	streamName := strings.Join(extraParts[0:len(extraParts)-1], "-")
	sha := extraParts[len(extraParts)-1]

	if err := validSha(sha); err != nil {
		return nil, err
	}

	return &streamSnapshot{streamName: streamName, kind: kind, sha: sha}, nil
}

// streamSnapshot represents a Snapshot that lives in a Stream
type streamSnapshot struct {
	sha        string
	kind       SnapshotKind
	streamName string
}

func (s *streamSnapshot) ID() snap.ID {
	return snap.ID(fmt.Sprintf(streamIDFmt, streamIDText, s.kind, s.streamName, s.sha))
}
func (s *streamSnapshot) Kind() SnapshotKind { return s.kind }
func (s *streamSnapshot) SHA() string        { return s.sha }

func (s *streamSnapshot) Download(db *DB) error {
	if err := db.shaPresent(s.SHA()); err == nil {
		// Already present!
		return nil
	}

	// TODO(dbentley): what if we've already fetched recently? We should figure out some way to
	// prevent that

	if db.stream == nil {
		return fmt.Errorf("cannot download snapshot %s: no streams configured", s.ID())
	}

	if err := db.stream.updateStream(s.streamName, db); err != nil {
		return err
	}

	return db.shaPresent(s.SHA())
}

func (s *streamSnapshot) DownloadTempRepo(_ *DB) (*repo.Repository, error) {
	return nil, errors.New("DownloadTempRepo unimplemented in streamSnapshot")
}

// updateStream updates the named stream
// the stream name is used to make sure we're operating on the right remote/refspec
// the sha is optionally used to override refspec for remote with a specific sha request
func (b *streamBackend) updateStream(name string, db *DB) error {
	b.stat.Counter(stats.GitStreamUpdateFetches).Inc(1)

	args := []string{"fetch", b.cfg.Remote}
	if !strings.HasPrefix(name, b.cfg.Name) {
		return fmt.Errorf("cannot update stream %s: does not match stream %s", name, db.stream.cfg.Name)
	}
	if strings.HasPrefix(name, b.cfg.Name+":") {
		// If the stream name includes a ref then fetch will override the default refspec
		args = append(args, strings.Replace(name, b.cfg.Name+":", "", 1))
	}
	_, err := db.dataRepo.Run(args...)
	return err
}
