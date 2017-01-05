package gitdb

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/scootdev/scoot/os/temp"
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

	actual, err := ioutil.ReadFile(filepath.Join(path, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(contents, actual) {
		t.Fatalf("bad contents: %q %q", contents, actual)
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
	data, err := ioutil.ReadFile(filepath.Join(co, "file.txt"))
	if err != nil || string(data) != "first" {
		t.Fatalf("error reading file.txt: %q %v (expected \"first\" <nil>)", data, err)
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
	data, err = ioutil.ReadFile(filepath.Join(co, "file.txt"))
	if err != nil || string(data) != "second" {
		t.Fatalf("error reading file.txt: %q %v (expected \"second\" <nil>)", data, err)
	}

	data, err = ioutil.ReadFile(filepath.Join(co, "scratch.txt"))
	if err == nil {
		t.Fatalf("scratch.txt existed in %v with contents %q; should not exist", co, data)
	}

	if err := fixture.db.ReleaseCheckout(co); err != nil {
		t.Fatal(err)
	}
}

func TestStream(t *testing.T) {
	// streamID := "stream-source_master-" + f.upstreamCommit1ID

}

type dbFixture struct {
	tmp               *temp.TempDir
	db                *DB
	external          *repo.Repository
	commit1ID         string
	commit2ID         string
	upstream          *repo.Repository
	upstreamCommit1ID string
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

	upstreamRepo, err := createRepo(tmp, "upstream-repo")
	if err != nil {
		return nil, err
	}

	if _, err := commitText(upstreamRepo, "upstream_zeroeth"); err != nil {
		return nil, err
	}

	if _, err := dataRepo.Run("remote", "add", "upstream", upstreamRepo.Dir()); err != nil {
		return nil, err
	}

	if _, err := dataRepo.Run("fetch", "upstream"); err != nil {
		return nil, err
	}

	stream := &StreamConfig{
		Name:    "source_master",
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
