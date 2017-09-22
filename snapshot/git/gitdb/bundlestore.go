package gitdb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"

	log "github.com/sirupsen/logrus"

	snap "github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/bundlestore"
)

// BundlestoreConfig defines how to talk to Bundlestore
type BundlestoreConfig struct {
	Store bundlestore.Store
}

type bundlestoreBackend struct {
	cfg *BundlestoreConfig
}

const bundlestoreIDText = "bs"

// "bs-gc-<bundle>-<stream>-<sha>"

func (b *bundlestoreBackend) parseID(id snap.ID, kind SnapshotKind, extraParts []string) (snapshot, error) {
	if b.cfg == nil {
		return nil, errors.New("Bundlestore backend not initialized.")
	}

	if len(extraParts) != 3 {
		return nil, fmt.Errorf("cannot parse snapshot ID: expected 5 parts in bundlestore id: %s", id)
	}
	bundleKey, streamName, sha := extraParts[0], extraParts[1], extraParts[2]

	if err := validSha(sha); err != nil {
		return nil, err
	}

	return &bundlestoreSnapshot{kind: kind, sha: sha, bundleKey: bundleKey, streamName: streamName}, nil
}

func (b *bundlestoreBackend) upload(s snapshot, db *DB) (snapshot, error) {
	// We only have to upload a localSnapshot
	switch s := s.(type) {
	case *tagsSnapshot:
		return s, nil
	case *streamSnapshot:
		// TODO(dbentley): we should upload to bundlestore if this commit is so recent
		// it might not be on every worker already.
		return s, nil
	case *bundlestoreSnapshot:
		return s, nil
	case *localSnapshot:
		return b.uploadLocalSnapshot(s, db)
	default:
		return nil, fmt.Errorf("cannot upload %v: unknown type %T", s, s)
	}
}

// git bundle create takes a rev list; it requires that it include a ref
// so we can't just do:
// git bundle create 545c88d71d40a49ebdfb1d268c724110793330d2..3060a3a519888957e13df75ffd09ea50f97dd03b
// instead, we have to write a temporary ref
// (nb: the subtracted revisions can be a commit, not a ref, so you can do:
// git bundle create 545c88d71d40a49ebdfb1d268c724110793330d2..master
// )
const bundlestoreTempRef = "reserved_scoot/bundlestore/__temp_for_writing"

func (b *bundlestoreBackend) uploadLocalSnapshot(s *localSnapshot, db *DB) (sn snapshot, err error) {
	// the sha of the commit we're going to use as the ref
	commitSha := s.sha

	// the revList to create the bundle
	// unless we can find a merge base, we'll just include the commit
	revList := bundlestoreTempRef

	// the name of the stream that this bundle requires
	streamName := ""

	switch s.kind {
	case KindGitCommitSnapshot:
		// For a git commit, we want a bundle that has just the diff compared to the stream
		// so find the merge base with our stream

		// The generated bundle will require either no prereqs or a commit that is in the stream
		if db.stream.cfg != nil && db.stream.cfg.RefSpec != "" {
			streamHead, err := db.dataRepo.RunSha("rev-parse", db.stream.cfg.RefSpec)
			if err != nil {
				return nil, err
			}

			mergeBase, err := db.dataRepo.RunSha("merge-base", streamHead, commitSha)

			if mergeBase == commitSha {
				// we were asked to ingest a sha that's on the stream,
				// e.g., the user just created a branch and just wants to test
				// the baseline, and hasn't modified anything yet.
				// so we don't have to upload it, just return that snapshot
				// if we don't do this, then our git bundle create will die
				// because the bundle would be empty.
				return &streamSnapshot{sha: commitSha, kind: KindGitCommitSnapshot, streamName: db.stream.cfg.Name}, nil
			}

			// if err != nil, it just means we don't have a merge-base
			if err == nil {
				revList = fmt.Sprintf("%s..%s", mergeBase, bundlestoreTempRef)
				streamName = db.stream.cfg.Name
			}

		}
	case KindFSSnapshot:
		// For an FSSnapshot (which is stored as a git tree), create a git commit
		// with no parent.
		// (Eventually we could get smarter, e.g., if it's storing the output of running
		// cmd foo, we could try to find another run of foo and use that as a parent
		// to reduce the delta)

		// The generated bundle will require no prereqs.
		commitSha, err = db.dataRepo.RunSha("commit-tree", commitSha, "-m",
			"commit to distribute GitDB FSSnapshot via bundlestore")
	default:
		return nil, fmt.Errorf("unknown Snapshot kind: %v", s.kind)
	}

	// update the ref
	if _, err := db.dataRepo.Run("update-ref", bundlestoreTempRef, commitSha); err != nil {
		return nil, err
	}

	d, err := db.tmp.TempDir("bundle-")
	if err != nil {
		return nil, err
	}

	bundleName := makeBundleName(s.sha)

	// we can't use tmpDir.TempFile() because we need the file to not exist
	bundleFilename := path.Join(d.Dir, bundleName)

	// create the bundle
	// -c core.packobjectedgesonlyshallow=0 is because our internal git
	// has a bug that shows up as:
	// $ git bundle create /tmp/mybundle.pack HEAD^..HEAD
	// fatal: expected sha1, got garbage:
	//  ^a7c35c3e44ca591d7dd98860ce49601dbc20a22c
	//
	// error: pack-objects died
	// so we pass it, but hope to remove it once the bug is fixed
	if _, err := db.dataRepo.Run("-c", "core.packobjectedgesonlyshallow=0", "bundle", "create", bundleFilename, revList); err != nil {
		return nil, err
	}

	_, err = b.uploadFile(bundleFilename, nil)
	if err != nil {
		return nil, err
	}

	// For now, our bundle key is always the sha of the object we are uploading.
	// We might eventually want to upload multiple objects in one bundle. E.g.,
	// for code review you might want to have both before and after snapshots. In that case,
	// we could upload once and return two IDs that have the same bundleKey but different
	// sha's.
	return &bundlestoreSnapshot{sha: s.sha, kind: s.Kind(), bundleKey: s.sha, streamName: streamName}, nil
}

func CreateBundlestoreSnapshot(sha string, kind SnapshotKind, bundleKey, streamName string) snapshot {
	return &bundlestoreSnapshot{sha: sha, kind: kind, bundleKey: bundleKey, streamName: streamName}
}

type bundlestoreSnapshot struct {
	sha  string
	kind SnapshotKind
	// the (shortened) key to use in our key value store
	// we actually store it as a name with a bit more around it,
	// e.g., bundleKey is the sha, and then the name we send on the wire
	// (generated by makeBundleName) is bs-<sha>.bundle
	bundleKey  string
	streamName string
}

func (s *bundlestoreSnapshot) ID() snap.ID {
	return snap.ID(strings.Join([]string{bundlestoreIDText, string(s.kind), s.bundleKey, s.streamName, s.sha}, "-"))
}
func (s *bundlestoreSnapshot) Kind() SnapshotKind { return s.kind }
func (s *bundlestoreSnapshot) SHA() string        { return s.sha }

func (s *bundlestoreSnapshot) Download(db *DB) error {
	log.Infof("Downloading sha: %s", s.SHA())
	if err := db.shaPresent(s.SHA()); err == nil {
		log.Infof("We already have sha: %s, returning from Download()", s.SHA())
		return nil
	}

	// TODO(dbentley): keep stats about bundlestore downloading
	// TODO(dbentley): keep stats about how long it takes to unbundle
	filename, err := s.downloadBundle(db)
	if err != nil {
		log.Info("Unable to download bundle: ", err)
		return err
	}

	// unbundle optimistically
	// this will succeed if we have all of the prerequisite objects

	if _, err = db.dataRepo.Run("bundle", "unbundle", filename); err == nil {
		log.Infof("Unbundling got the sha: %s, returning from Download()", s.SHA())
		return db.shaPresent(s.sha)
	}

	// we couldn't unbundle
	// see if it's because we're missing prereqs
	if exitError := err.(*exec.ExitError); exitError == nil || !strings.Contains(string(exitError.Stderr), "error: Repository lacks these prerequisite commits:") {
		log.Info("Can't find sha: ", s.SHA(), " and prereqs aren't the problem, returning err: ", err.Error())
		return err
	}

	// we are missing prereqs, so let's try updating the stream that's the basis of the bundle
	// this likely happened because:
	// we're in a worker that started at time T1, when master pointed at commit C1
	// at time T2, a commit C2 was created in our stream
	// at time T3, a user ingested a git commit C3 whose ancestor is C2
	// GitDB in their scoot-snapshot-db picked a merge-base of C2, because T3-T2 was sufficiently
	// large (say, a half hour) that it's reasonable to assume its easy to get.
	// Now we've got the bundle for C3, which depends on C2, but we only have C1, so we have to
	// update our stream.
	if err := db.stream.updateStream(s.streamName, db); err != nil {
		log.Infof("Couldn't download sha: %s, updateStream returned error: %s", s.SHA(), err.Error())
		return err
	}

	if _, err := db.dataRepo.Run("bundle", "unbundle", filename); err != nil {
		// if we still can't unbundle, then the bundle might be corrupt or the
		// prereqs might not be in the stream, or maybe the git server is serving us
		// stale data.
		log.Infof("Couldn't download sha: %s, the final unbundling attempt returned error: %s", s.SHA(), err.Error())
		return err
	}

	return db.shaPresent(s.sha)
}

func (s *bundlestoreSnapshot) downloadBundle(db *DB) (filename string, err error) {
	d, err := db.tmp.TempDir("bundle-")
	if err != nil {
		return "", err
	}
	bundleName := makeBundleName(s.bundleKey)
	bundleFilename := path.Join(d.Dir, bundleName)
	f, err := os.Create(bundleFilename)
	if err != nil {
		return "", err
	}
	defer f.Close()

	r, err := db.bundles.cfg.Store.OpenForRead(bundleName)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(f, r); err != nil {
		return "", err
	}

	return f.Name(), nil
}

func makeBundleName(key string) string {
	return fmt.Sprintf("bs-%s.bundle", key)
}

func (b *bundlestoreBackend) uploadFile(filePath string, ttl *bundlestore.TTLValue) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	name := path.Base(filePath)
	if name == "." || name == "/" {
		return "", fmt.Errorf("Invalid path %v, base parsed to %v", filePath, name)
	}

	if err := b.cfg.Store.Write(name, f, ttl); err != nil {
		return "", err
	}

	return b.cfg.Store.Root() + name, nil
}
