package gitfiler

import (
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

func TestCheckouter(t *testing.T) {
	tmp, err := temp.NewTempDir("", "checkouter_test")
	if err != nil {
		t.Fatal(err)
	}

	var id1, id2 string
	repo, err := CreateReferenceRepo(tmp, &id1, &id2)
	if err != nil {
		t.Fatal(err)
	}
	doneCh := make(chan struct{})
	defer close(doneCh)
	checkouter := NewRefRepoCloningCheckouter(&ConstantIniter{repo}, tmp, doneCh, 0)
	c1, err := checkouter.Checkout(id1)
	if err != nil {
		t.Fatalf("error checking out %v, %v", id1, err)
	}
	c2, err := checkouter.Checkout(id2)
	if err != nil {
		t.Fatalf("error checking out %v, %v", id2, err)
	}
	if c1.Path() == c2.Path() {
		t.Fatalf("checkouts should have separate paths: %v %v", c1.Path(), c2.Path())
	}

	// Make sure they have separate file contents at the same time
	data, err := ioutil.ReadFile(filepath.Join(c1.Path(), "file.txt"))
	if err != nil || string(data) != "first" {
		t.Fatalf("error reading file.txt: %q %v (expected \"first\" <nil>)", data, err)
	}

	data, err = ioutil.ReadFile(filepath.Join(c2.Path(), "file.txt"))
	if err != nil || string(data) != "second" {
		t.Fatalf("error reading file.txt: %q %v (expected \"second\" <nil>)", data, err)
	}

	// Write temporary data into checkouts to make sure it's cleaned
	if err = ioutil.WriteFile(filepath.Join(c1.Path(), "scratch.txt"), []byte("1"), 0777); err != nil {
		t.Fatal(err)
	}
	if err = ioutil.WriteFile(filepath.Join(c2.Path(), "scratch.txt"), []byte("2"), 0777); err != nil {
		t.Fatal(err)
	}

	c1.Release()
	c2.Release()

	c3, err := checkouter.Checkout(id1)
	if err != nil {
		t.Fatalf("error checking out %v, %v", id1, err)
	}

	data, err = ioutil.ReadFile(filepath.Join(c3.Path(), "scratch.txt"))
	if err == nil {
		t.Fatalf("scratch.txt existed in %v with contents %q; should not exist", id1, data)
	}

	c3.Release()

	_, err = checkouter.Checkout("Not a valid git sha1")
	if err == nil {
		t.Fatalf("should not have been able to check out; %v", c1)
	}
}

func CreateReferenceRepo(tmp *temp.TempDir, id1 *string, id2 *string) (*repo.Repository, error) {
	// git init
	dir, err := tmp.TempDir("ref-repo-")
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

	// Create a commit with file.txt = "first"
	filename := filepath.Join(dir.Dir, "file.txt")
	if err = ioutil.WriteFile(filename, []byte("first"), 0777); err != nil {
		return nil, err
	}

	if _, err = r.Run("add", "file.txt"); err != nil {
		return nil, err
	}
	// Run it with just this thing
	if _, err = r.Run("commit", "-am", "first post"); err != nil {
		return nil, err
	}
	if id, err := r.RunSha("rev-parse", "HEAD"); err != nil {
		return nil, err
	} else {
		*id1 = id
	}

	// Create a commit with file.txt = "second"
	if err = ioutil.WriteFile(filename, []byte("second"), 0777); err != nil {
		return nil, err
	}
	if _, err = r.Run("commit", "-am", "second post"); err != nil {
		return nil, err
	}
	if id, err := r.RunSha("rev-parse", "HEAD"); err != nil {
		return nil, err
	} else {
		*id2 = id
	}

	return r, nil
}
