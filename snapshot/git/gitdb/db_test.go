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
	"github.com/scootdev/scoot/snapshot"
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

	id, err := fixture.db.IngestDir(ingestDir.Dir)
	if err != nil {
		t.Fatal(err)
	}

	path, err := fixture.db.Checkout(id)
	if err != nil {
		t.Fatal(err)
	}

	if err := assertFileContents(path, "foo.txt", string(contents)); err != nil {
		t.Fatal(err)
	}

	if err := fixture.db.ReleaseCheckout(path); err != nil {
		t.Fatal(err)
	}
}

func TestIngestCommit(t *testing.T) {
	id1, err := fixture.db.IngestGitCommit(fixture.external, fixture.commit1ID)
	if err != nil {
		t.Fatal(err)
	}

	id2, err := fixture.db.IngestGitCommit(fixture.external, fixture.commit2ID)
	if err != nil {
		t.Fatal(err)
	}

	co, err := fixture.db.Checkout(id1)
	if err != nil {
		t.Fatalf("error checking out %v, %v", id1, err)
	}

	// test contents
	if err := assertFileContents(co, "file.txt", "first"); err != nil {
		t.Fatal(err)
	}

	// Write temporary data into checkouts to make sure it's cleaned
	if err = ioutil.WriteFile(filepath.Join(co, "scratch.txt"), []byte("1"), 0777); err != nil {
		t.Fatal(err)
	}

	if err := fixture.db.ReleaseCheckout(co); err != nil {
		t.Fatal(err)
	}

	co, err = fixture.db.Checkout(id2)
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

	if err := fixture.db.ReleaseCheckout(co); err != nil {
		t.Fatal(err)
	}
}

func TestStream(t *testing.T) {
	upstreamCommit1ID, err := commitText(fixture.upstream, "upstream_first")
	if err != nil {
		t.Fatal(err)
	}

	streamID := snapshot.ID("stream-swh-sm-" + upstreamCommit1ID)

	id, err := fixture.db.Download(streamID)
	if err != nil {
		t.Fatal(err)
	}

	co, err := fixture.db.Checkout(id)
	if err != nil {
		t.Fatal(err)
	}

	if err := assertFileContents(co, "file.txt", "upstream_first"); err != nil {
		t.Fatal(err)
	}

}

type dbFixture struct {
	tmp       *temp.TempDir
	db        *DB
	external  *repo.Repository
	commit1ID string
	commit2ID string
	upstream  *repo.Repository
}

func (f *dbFixture) close() {
	f.db.Close()
}

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

	commit1ID, err := commitText(external, "first")
	if err != nil {
		return nil, err
	}

	commit2ID, err := commitText(external, "second")
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

	if _, err := dataRepo.Run("remote", "add", "upstream", upstream.Dir()); err != nil {
		return nil, err
	}

	if _, err := dataRepo.Run("fetch", "upstream"); err != nil {
		return nil, err
	}

	stream := &StreamConfig{
		Name:    "sm",
		Remote:  "upstream",
		RefSpec: "refs/remotes/upstream/master",
	}

	db := MakeDB(dataRepo, tmp, stream)

	return &dbFixture{
		tmp:       tmp,
		db:        db,
		external:  external,
		commit1ID: commit1ID,
		commit2ID: commit2ID,
		upstream:  upstream,
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
