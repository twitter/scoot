package bazel

import (
	"os/exec"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/os/temp"
)

func init() {
	log.AddHook(hooks.NewContextHook())
}

func makeTestingFiler() *bzFiler {
	localStore, err := temp.TempDirDefault()
	if err != nil {
		log.Fatal(err)
	}
	localStorePathFn := func(bc *bzCommand) {
		bc.localStorePath = localStore.Dir
	}
	bf := MakeBzFilerWithOptions(localStorePathFn)
	return bf
}

// directory save tests

func TestSaveEmptyDir(t *testing.T) {
	root, err := temp.TempDirDefault()
	if err != nil {
		t.Fatal(err)
	}
	tmpDirPrefix := "tmp"
	tmpDir, err := root.TempDir(tmpDirPrefix)
	if err != nil {
		t.Fatal(err)
	}

	bf := makeTestingFiler()
	id, err := bf.Ingest(tmpDir.Dir)
	if err != nil {
		t.Fatal(err)
	}

	if id != emptyID {
		t.Fatalf("Expected id to be %s, was %s", emptyID, id)
	}
}

func TestSaveDir(t *testing.T) {
	root, err := temp.TempDirDefault()
	if err != nil {
		t.Fatal(err)
	}
	tmpDirPrefix := "tmp"
	tmpDir, err := root.TempDir(tmpDirPrefix)
	if err != nil {
		t.Fatal(err)
	}
	f, err := tmpDir.TempFile("nonempty")
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("some words\n")
	if err != nil {
		t.Fatal(err)
	}

	bf := makeTestingFiler()
	id, err := bf.Ingest(tmpDir.Dir)
	if err != nil {
		t.Fatal(err)
	}

	// TODO: check for actual ID
	if id == emptyID {
		t.Fatalf("Expected id to not be %s, was %s", emptyID, id)
	}
	size, err := getSize(id)
	if err != nil {
		t.Fatal(err)
	}
	if size <= emptySize {
		t.Fatalf("Expected size to be >%d, ID: %s", emptySize, id)
	}
	sha, err := getSha(id)
	if err != nil {
		t.Fatal(err)
	}
	if sha == emptySha {
		t.Fatalf("Expected sha to not be %s. ID: %s", emptySha, id)
	}
}

// file save tests

func TestSaveEmptyFile(t *testing.T) {
	root, err := temp.TempDirDefault()
	if err != nil {
		t.Fatal(err)
	}
	f, err := root.TempFile("empty")
	if err != nil {
		t.Fatal(err)
	}

	bf := makeTestingFiler()
	id, err := bf.Ingest(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	if id != emptyID {
		t.Fatalf("Expected id to be %s, was %s", id, emptyID)
	}
}

func TestSaveFile(t *testing.T) {
	root, err := temp.TempDirDefault()
	if err != nil {
		t.Fatal(err)
	}
	f, err := root.TempFile("nonempty")
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("some words\n")
	if err != nil {
		t.Fatal(err)
	}

	bf := makeTestingFiler()
	id, err := bf.Ingest(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	// TODO: check for actual ID
	if id == emptyID {
		t.Fatalf("Expected id to not be %s, was %s", id, emptyID)
	}
	size, err := getSize(id)
	if err != nil {
		t.Fatal(err)
	}
	if size <= emptySize {
		t.Fatalf("Expected size to be >%d, ID: %s", emptySize, id)
	}
	sha, err := getSha(id)
	if err != nil {
		t.Fatal(err)
	}
	if sha == emptySha {
		t.Fatalf("Expected sha to not be %s. ID: %s", emptySha, id)
	}

}

// directory materialize test

func TestMaterializeDir(t *testing.T) {
	root, err := temp.TempDirDefault()
	if err != nil {
		t.Fatal(err)
	}
	tmpDirPrefix := "tmp"
	tmpDir, err := root.TempDir(tmpDirPrefix)
	if err != nil {
		t.Fatal(err)
	}
	f, err := tmpDir.TempFile("nonempty")
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("some words\n")
	if err != nil {
		t.Fatal(err)
	}

	bf := makeTestingFiler()
	id, err := bf.Ingest(tmpDir.Dir)
	if err != nil {
		t.Fatal(err)
	}

	co, err := bf.Checkout(id)
	if err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("diff", "-r", co.Path(), root.Dir).Output()
	if string(output) != "" {
		t.Fatalf("Expected %s and %s to be equivalent, instead received \"%s\" from command", co.Path(), tmpDir.Dir, string(output))
	}
	if err != nil {
		t.Fatal(err)
	}
}
