package gitdb

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/os/temp"
	snap "github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/bundlestore"
	"github.com/twitter/scoot/snapshot/git/repo"
)

var fixture *dbFixture

func TestIngestDir(t *testing.T) {
	ingestDir, err := fixture.tmp.TempDir("ingest_dir")
	if err != nil {
		t.Fatal(err)
	}

	contents := "bar"
	if err := writeFileText(ingestDir.Dir, "foo.txt", contents); err != nil {
		t.Fatal(err)
	}

	id, err := fixture.simpleDB.IngestDir(ingestDir.Dir)
	if err != nil {
		t.Fatal(err)
	}

	path, err := fixture.simpleDB.Checkout(id)
	if err != nil {
		t.Fatal(err)
	}

	if err := assertFileContents(path, "foo.txt", contents); err != nil {
		t.Fatal(err)
	}

	if err := fixture.simpleDB.ReleaseCheckout(path); err != nil {
		t.Fatal(err)
	}
}

func TestIngestCommit(t *testing.T) {
	commit1ID, err := commitText(fixture.external, "first")
	if err != nil {
		t.Fatal(err)
	}

	commit2ID, err := commitText(fixture.external, "second")
	if err != nil {
		t.Fatal(err)
	}

	id1, err := fixture.simpleDB.IngestGitCommit(fixture.external, commit1ID)
	if err != nil {
		t.Fatal(err)
	}

	id2, err := fixture.simpleDB.IngestGitCommit(fixture.external, commit2ID)
	if err != nil {
		t.Fatal(err)
	}

	co, err := fixture.simpleDB.Checkout(id1)
	if err != nil {
		t.Fatalf("error checking out %v, %v", id1, err)
	}
	defer fixture.simpleDB.ReleaseCheckout(co)

	// test contents
	if err := assertFileContents(co, "file.txt", "first"); err != nil {
		t.Fatal(err)
	}

	// Write temporary data into checkouts to make sure it's cleaned
	if err := writeFileText(co, "scratch.txt", "1"); err != nil {
		t.Fatal(err)
	}

	if err := fixture.simpleDB.ReleaseCheckout(co); err != nil {
		t.Fatal(err)
	}

	co, err = fixture.simpleDB.Checkout(id2)
	if err != nil {
		t.Fatalf("error checking out %v, %v", id2, err)
	}

	// test contents
	if err := assertFileContents(co, "file.txt", "second"); err != nil {
		t.Fatal(err)
	}

	_, err = os.Open(filepath.Join(co, "scratch.txt"))
	if err == nil {
		t.Fatalf("scratch.txt existed in %v; should not exist", co)
	}

	// Write temporary data into checkouts to make sure we can ingest uncommitted data.
	if err := writeFileText(fixture.external.Dir(), "scratch.txt", "1"); err != nil {
		t.Fatal(err)
	}

	id3, err := fixture.simpleDB.IngestGitWorkingDir(fixture.external)
	if err != nil {
		t.Fatal(err)
	}

	if err := fixture.simpleDB.ReleaseCheckout(co); err != nil {
		t.Fatal(err)
	}

	co, err = fixture.simpleDB.Checkout(id3)
	if err != nil {
		t.Fatalf("error checking out %v, %v", id3, err)
	}

	// test contents
	if err := assertFileContents(co, "file.txt", "second"); err != nil {
		t.Fatal(err)
	}

	_, err = os.Open(filepath.Join(co, "scratch.txt"))
	if err != nil {
		t.Fatalf("scratch.txt should have existed in %v", co)
	}

}

func TestStream(t *testing.T) {
	// Create a commit in upstream, then check it out in our DB and compare contents.

	upstreamCommit1ID, err := commitText(fixture.upstream, "upstream_first")
	if err != nil {
		t.Fatal(err)
	}

	streamID := fixture.simpleDB.IDForStreamCommitSHA("sm", upstreamCommit1ID)

	co, err := fixture.simpleDB.Checkout(streamID)
	if err != nil {
		t.Fatal(err)
	}

	defer fixture.simpleDB.ReleaseCheckout(co)

	if err := assertFileContents(co, "file.txt", "upstream_first"); err != nil {
		t.Fatal(err)
	}

}

func TestInitUpdate(t *testing.T) {
	// This test doesn't use our fixture DBs because it has such specific git setup
	// Our git repos are:
	// rw: the main, upstream read-write repo
	// mirror: where to init from (actually not a repo; just a bundle)
	// ro: a read-only version of rw
	// data: the data repo for our db

	// our initer will fetch from the bundle "mirror"

	// create empty repos for rw, ro, and data
	rw, err := createRepo(fixture.tmp, "rw-repo")
	if err != nil {
	}

	ro, err := createRepo(fixture.tmp, "ro-repo")
	if err != nil {
		t.Fatal(err)
	}

	// add ro as a remote to data
	if _, err := ro.Run("remote", "add", "origin", rw.Dir()); err != nil {
		t.Fatal(err)
	}

	// create a commit "first" in rw
	firstCommitID, err := commitText(rw, "stream_init_first")
	if err != nil {
		t.Fatal(err)
	}

	// create a bundle with rw's master; set it as mirror
	f, err := fixture.tmp.TempFile("bundle")
	if err != nil {
		t.Fatal(err)
	}
	mirror := f.Name()
	f.Close()

	_, err = rw.Run("bundle", "create", mirror, "master")
	if err != nil {
		t.Fatal(err)
	}

	// create a commit "second" in rw
	secondCommitID, err := commitText(rw, "stream_init_second")
	if err != nil {
		t.Fatal(err)
	}

	streamCfg := &StreamConfig{
		Name:    "sro", // short for Source ReadOnly
		Remote:  "ro",
		RefSpec: "refs/remotes/ro/master",
	}

	db := MakeDBNewRepo(&bundleIniter{mirror, ro}, &pullUpdater{rw.Dir()},
		fixture.tmp, streamCfg, nil, nil, AutoUploadNone, stats.NilStatsReceiver())
	defer db.Close()

	firstID := db.IDForStreamCommitSHA("sro", firstCommitID)
	secondID := db.IDForStreamCommitSHA("sro", secondCommitID)

	// db.Checkout(first) will work only if initer is working
	//   (because data's only upstream is ro, which doesn't have first at all)
	co, err := db.Checkout(firstID)
	if err != nil {
		t.Fatal(err)
	}
	db.ReleaseCheckout(co)

	// db.Checkout(second) should fail
	if _, err = db.Checkout(secondID); err == nil {
		t.Fatal("shouldn't be able to see second")
	}

	// pull from rw to ro
	err = db.Update()
	if err != nil {
		t.Fatal(err)
	}

	// db.Checkout(second) should succeed
	co, err = db.Checkout(secondID)
	if err != nil {
		t.Fatal(err)
	}
	db.ReleaseCheckout(co)
}

func TestInitFails(t *testing.T) {
	streamCfg := &StreamConfig{
		Name:    "sro",
		Remote:  "ro",
		RefSpec: "refs/remotes/ro/master",
	}

	db := MakeDBNewRepo(&bundleIniter{"/dev/null", fixture.upstream}, nil,
		fixture.tmp, streamCfg, nil, nil, AutoUploadNone, stats.NilStatsReceiver())
	defer db.Close()

	ingestDir, err := fixture.tmp.TempDir("ingest_dir")
	if err != nil {
		t.Fatal(err)
	}

	contents := []byte("bar")
	filename := filepath.Join(ingestDir.Dir, "foo.txt")

	if err = ioutil.WriteFile(filename, contents, 0777); err != nil {
		t.Fatal(err)
	}

	if _, err = db.IngestDir(ingestDir.Dir); err == nil {
		t.Fatal("no error")
	}
}

func TestClean(t *testing.T) {
	tmp, err := temp.NewTempDir("", "db_test")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		os.RemoveAll(tmp.Dir)
	}()

	r, err := createRepo(tmp, "temp_repo")
	if err != nil {
		t.Fatal(err)
	}

	contents := "baz"
	gitDir := filepath.Join(r.Dir(), ".git")
	if err := writeFileText(r.Dir(), "foo.txt", contents); err != nil {
		t.Fatal(err)
	}
	if err := writeFileText(gitDir, "index.lock", ""); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(gitDir, "index.lock")); err != nil {
		t.Fatal(err)
	}

	r.CleanupKill()

	if err := assertFileContents(r.Dir(), "foo.txt", contents); err == nil {
		t.Fatal("Expected file foo.txt would not exist")
	}
	if _, err := os.Stat(filepath.Join(gitDir, "index.lock")); !os.IsNotExist(err) {
		t.Fatalf("Expected .git/index.lock file would not exist: %v\n", err)
	}
}

type bundleIniter struct {
	mirror string
	ro     *repo.Repository
}

func (i *bundleIniter) Init() (*repo.Repository, error) {
	dataRepo, err := createRepo(fixture.tmp, "data-repo")
	if err != nil {
		return nil, err
	}

	if _, err := dataRepo.Run("remote", "add", "ro", i.ro.Dir()); err != nil {
		return nil, err
	}

	if _, err := dataRepo.Run("pull", i.mirror, "master"); err != nil {
		return nil, err
	}

	return dataRepo, nil
}

type pullUpdater struct {
	rwDir string
}

func (p *pullUpdater) UpdateInterval() time.Duration {
	return time.Duration(0) * time.Second
}

// When used with a repo set up by bundleIniter, the only remote we have is the original
// bundle, so we add the real remote repo to pull from. (We could also have re-written the bundle)
func (p *pullUpdater) Update(r *repo.Repository) error {
	if _, err := r.Run("remote", "add", "origin", p.rwDir); err != nil {
		return err
	}
	_, err := r.Run("pull", "origin", "master")
	return err
}

func TestTags(t *testing.T) {
	// Create a commit in external, then ingest into authorDB, then check it out in our regular db

	externalCommitID, err := commitText(fixture.external, "tags_first")
	if err != nil {
		t.Fatal(err)
	}

	id, err := fixture.authorDB.IngestGitCommit(fixture.external, externalCommitID)
	if err != nil {
		t.Fatal(err)
	}

	co, err := fixture.consumerDB.Checkout(id)
	if err != nil {
		t.Fatal(err)
	}

	defer fixture.consumerDB.ReleaseCheckout(co)

	if err := assertFileContents(co, "file.txt", "tags_first"); err != nil {
		t.Fatal(err)
	}
}

func TestBundlestore(t *testing.T) {
	authorDataRepo, err := createRepo(fixture.tmp, "author-data-repo")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := authorDataRepo.Run("remote", "add", "upstream", fixture.upstream.Dir()); err != nil {
		t.Fatal(err)
	}

	if _, err := authorDataRepo.Run("fetch", "upstream"); err != nil {
		t.Fatal(err)
	}

	streamCfg := &StreamConfig{
		Name:    "sm",
		Remote:  "upstream",
		RefSpec: "refs/remotes/upstream/master",
	}

	tmp, err := fixture.tmp.TempDir("bundles")
	if err != nil {
		t.Fatal(err)
	}
	store, err := bundlestore.MakeFileStore(tmp.Dir)
	if err != nil {
		t.Fatal(err)
	}

	bundleCfg := &BundlestoreConfig{
		Store: store,
	}

	authorDB := MakeDBFromRepo(authorDataRepo, nil, fixture.tmp,
		streamCfg, nil, bundleCfg, AutoUploadBundlestore, stats.NilStatsReceiver())

	consumerDataRepo, err := createRepo(fixture.tmp, "consumer-data-repo")
	if err != nil {
		t.Fatal(err)
	}

	// First, test that if we try and upload something that's already in master, we succeed
	if _, err := consumerDataRepo.Run("remote", "add", "upstream", fixture.upstream.Dir()); err != nil {
		t.Fatal(err)
	}

	consumerDB := MakeDBFromRepo(consumerDataRepo, nil, fixture.tmp,
		streamCfg, nil, bundleCfg, AutoUploadBundlestore, stats.NilStatsReceiver())

	upstreamMaster, err := fixture.upstream.RunSha("rev-parse", "master")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := fixture.external.Run("fetch", "upstream"); err != nil {
		t.Fatal(err)
	}

	id, err := authorDB.IngestGitCommit(fixture.external, upstreamMaster)
	if err != nil {
		t.Fatal(err)
	}

	// Now, create a change in external with new contents and upload that
	externalCommitID, err := commitText(fixture.external, "bundlestore_first")
	if err != nil {
		t.Fatal(err)
	}

	id, err = authorDB.IngestGitCommit(fixture.external, externalCommitID)
	if err != nil {
		t.Fatal(err)
	}

	// TODO(dbentley): test the bundle has only the changed objects
	if err := assertSnapshotContents(consumerDB, id, "file.txt", "bundlestore_first"); err != nil {
		t.Fatal(err)
	}

	// Now, create a new directory with independent contents and ingest it.
	tmp, err = fixture.tmp.TempDir("output")
	if err != nil {
		t.Fatal(err)
	}

	if err := writeFileText(tmp.Dir, "stdout.txt", "stdout"); err != nil {
		t.Fatal(err)
	}
	if err := writeFileText(tmp.Dir, "stderr.txt", "stderr"); err != nil {
		t.Fatal(err)
	}

	id, err = consumerDB.IngestDir(tmp.Dir)
	if err != nil {
		t.Fatal(err)
	}

	if err := assertSnapshotContents(consumerDB, id, "stdout.txt", "stdout"); err != nil {
		t.Fatal(err)
	}
}

type dbFixture struct {
	tmp *temp.TempDir
	// simpleDB is the simplest DB; no auto-upload
	simpleDB *DB
	// authorDB is the DB where we'll author changes
	authorDB *DB
	// consumerDB is the DB where we'll consume changes (e.g., in the worker)
	consumerDB *DB
	external   *repo.Repository
	upstream   *repo.Repository
	tags       *repo.Repository
}

func (f *dbFixture) close() {
	f.simpleDB.Close()
	f.authorDB.Close()
	f.consumerDB.Close()
	os.RemoveAll(f.tmp.Dir)
}

// Create a new repo in tmp with directory name starting with name
func createRepo(tmp *temp.TempDir, name string) (*repo.Repository, error) {
	dir, err := tmp.TempDir(name)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command("git", "init")
	cmd.Dir = dir.Dir
	err = cmd.Run()
	if err != nil {
		return nil, fmt.Errorf("error init'ing: %v", err)
	}

	// create the repo
	r, err := repo.NewRepository(dir.Dir)
	if err != nil {
		return nil, err
	}

	if _, err = r.Run("config", "user.name", "Scoot Test"); err != nil {
		return nil, err
	}
	if _, err = r.Run("config", "user.email", "scoottest@twitter.github.io"); err != nil {
		return nil, err
	}

	return r, nil
}

// Make a commit in repo r with "file.txt" having contents text
func commitText(r *repo.Repository, text string) (string, error) {
	if err := writeFileText(r.Dir(), "file.txt", text); err != nil {
		return "", err
	}

	if _, err := r.Run("add", "file.txt"); err != nil {
		return "", err
	}

	if _, err := r.Run("commit", "-am", "created by commitText"); err != nil {
		return "", err
	}
	return r.RunSha("rev-parse", "HEAD")
}

func writeFileText(dir string, base string, text string) error {
	filename := filepath.Join(dir, base)
	return ioutil.WriteFile(filename, []byte(text), 0777)
}

// asserts file `base` in `dir` has contents `expected` or errors
func assertFileContents(dir string, base string, expected string) error {
	actualBytes, err := ioutil.ReadFile(filepath.Join(dir, base))
	if err != nil {
		return err
	}
	actual := string(actualBytes)
	if expected != actual {
		return fmt.Errorf("bad contents: %q %q", expected, actual)
	}
	return nil
}

// asserts file `base` in snapshot `id` (from `db`) has contents `expected` or errors
func assertSnapshotContents(db snap.DB, id snap.ID, base string, expected string) error {
	co, err := db.Checkout(id)
	if err != nil {
		return err
	}
	defer db.ReleaseCheckout(co)
	return assertFileContents(co, base, expected)
}

func setup() (f *dbFixture, err error) {
	// git init
	tmp, err := temp.NewTempDir("", "db_test")
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			os.RemoveAll(tmp.Dir)
		}
	}()

	external, err := createRepo(tmp, "external-repo")
	if err != nil {
		return nil, err
	}

	dataRepo, err := createRepo(tmp, "data-repo")
	if err != nil {
		return nil, err
	}

	upstream, err := createRepo(tmp, "upstream-repo")
	if err != nil {
		return nil, err
	}

	if _, err := commitText(upstream, "upstream_zeroeth"); err != nil {
		return nil, err
	}

	tags, err := createRepo(tmp, "tags-repo")
	if err != nil {
		return nil, err
	}

	if _, err := external.Run("remote", "add", "upstream", upstream.Dir()); err != nil {
		return nil, err
	}

	if _, err := dataRepo.Run("remote", "add", "upstream", upstream.Dir()); err != nil {
		return nil, err
	}

	if _, err := dataRepo.Run("remote", "add", "tags", tags.Dir()); err != nil {
		return nil, err
	}

	if _, err := dataRepo.Run("fetch", "upstream"); err != nil {
		return nil, err
	}

	streamCfg := &StreamConfig{
		Name:    "sm",
		Remote:  "upstream",
		RefSpec: "refs/remotes/upstream/master",
	}

	tagsCfg := &TagsConfig{
		Name:   "sss",
		Remote: "tags",
		Prefix: "scoot_reserved",
	}

	simpleDB := MakeDBFromRepo(dataRepo, nil, tmp, streamCfg, tagsCfg, nil, AutoUploadNone, stats.NilStatsReceiver())

	authorDataRepo, err := createRepo(tmp, "author-data-repo")
	if err != nil {
		return nil, err
	}

	if _, err := authorDataRepo.Run("remote", "add", "upstream", upstream.Dir()); err != nil {
		return nil, err
	}

	if _, err := authorDataRepo.Run("remote", "add", "tags", tags.Dir()); err != nil {
		return nil, err
	}

	if _, err := authorDataRepo.Run("fetch", "upstream"); err != nil {
		return nil, err
	}

	authorDB := MakeDBFromRepo(authorDataRepo, nil, tmp, streamCfg, tagsCfg, nil, AutoUploadTags, stats.NilStatsReceiver())

	consumerDataRepo, err := createRepo(tmp, "consumer-data-repo")
	if err != nil {
		return nil, err
	}

	if _, err := consumerDataRepo.Run("remote", "add", "upstream", upstream.Dir()); err != nil {
		return nil, err
	}

	if _, err := consumerDataRepo.Run("remote", "add", "tags", tags.Dir()); err != nil {
		return nil, err
	}

	consumerDB := MakeDBFromRepo(consumerDataRepo, nil, tmp, streamCfg, tagsCfg, nil, AutoUploadNone, stats.NilStatsReceiver())

	return &dbFixture{
		tmp:        tmp,
		simpleDB:   simpleDB,
		authorDB:   authorDB,
		consumerDB: consumerDB,
		external:   external,
		upstream:   upstream,
		tags:       tags,
	}, nil
}

// Pull setup into one place so we only create repos once.
func TestMain(m *testing.M) {
	flag.Parse()
	var err error
	fixture, err = setup()
	defer fixture.close()
	if err != nil {
		log.Fatal(err)
	}
	result := m.Run()
	// we need to call fixture.close() here because the defer we've registered
	// won't be hit after the os.Exit below
	fixture.close()
	os.Exit(result)
}
