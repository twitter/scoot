package gitdb

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/scootdev/scoot/os/temp"
	snap "github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

var fixture *dbFixture

func TestIngestDir(t *testing.T) {
	ingestDir, err := fixture.tmp.TempDir("ingest_dir")
	if err != nil {
		t.Fatal(err)
	}

	contents := []byte("bar")
	filename := filepath.Join(ingestDir.Dir, "foo.txt")

	if err = ioutil.WriteFile(filename, contents, 0777); err != nil {
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

	if err := assertFileContents(path, "foo.txt", string(contents)); err != nil {
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
	if err = ioutil.WriteFile(filepath.Join(co, "scratch.txt"), []byte("1"), 0777); err != nil {
		t.Fatal(err)
	}

	if err := fixture.simpleDB.ReleaseCheckout(co); err != nil {
		t.Fatal(err)
	}

	co, err = fixture.simpleDB.Checkout(id2)
	if err != nil {
		t.Fatalf("error checking out %v, %v", id2, err)
	}

	// text contents
	if err := assertFileContents(co, "file.txt", "second"); err != nil {
		t.Fatal(err)
	}

	_, err = os.Open(filepath.Join(co, "scratch.txt"))
	if err == nil {
		t.Fatalf("scratch.txt existed in %v; should not exist", co)
	}

}

func TestStream(t *testing.T) {
	// Create a commit in upstream, then check it out in our DB and compare contents.

	upstreamCommit1ID, err := commitText(fixture.upstream, "upstream_first")
	if err != nil {
		t.Fatal(err)
	}

	streamID := snap.ID("stream-gc-sm-" + upstreamCommit1ID)

	co, err := fixture.simpleDB.Checkout(streamID)
	if err != nil {
		t.Fatal(err)
	}

	defer fixture.simpleDB.ReleaseCheckout(co)

	if err := assertFileContents(co, "file.txt", "upstream_first"); err != nil {
		t.Fatal(err)
	}

}

func TestTags(t *testing.T) {
	// Create a commit in external, then ingest into authorDB, then check it out in our regular db

	externalCommitID, err := commitText(fixture.tags, "tags_first")
	if err != nil {
		t.Fatal(err)
	}

	id, err := fixture.authorDB.IngestGitCommit(fixture.tags, externalCommitID)
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
	if _, err = r.Run("config", "user.email", "scoottest@scootdev.github.io"); err != nil {
		return nil, err
	}

	return r, nil
}

// Make a commit in repo r with "file.txt" having contents text
func commitText(r *repo.Repository, text string) (string, error) {
	// Create a commit with file.txt = "first"
	filename := filepath.Join(r.Dir(), "file.txt")
	if err := ioutil.WriteFile(filename, []byte(text), 0777); err != nil {
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

func setup() (*dbFixture, error) {
	// git init
	tmp, err := temp.NewTempDir("", "db_test")
	if err != nil {
		return nil, err
	}

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

	simpleDB := MakeDB(dataRepo, tmp, streamCfg, tagsCfg, AutoUploadNone)

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

	authorDB := MakeDB(authorDataRepo, tmp, streamCfg, tagsCfg, AutoUploadTags)

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

	consumerDB := MakeDB(consumerDataRepo, tmp, streamCfg, tagsCfg, AutoUploadNone)

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
	if err != nil {
		log.Fatal(err)
	}
	result := m.Run()
	fixture.close()
	os.Exit(result)
}
