package bazel

import (
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
	bf := makeTestingFiler()
	id, err := bf.Ingest(root.Dir)
	if err != nil {
		t.Fatal(err)
	}
	if id != emptyID {
		t.Fatalf("Expected id to be %s, was %s", id, emptyID)
	}
}

func TestSaveDir(t *testing.T) {
	root, err := temp.TempDirDefault()
	if err != nil {
		t.Fatal(err)
	}
	tmpDirName := "tmp"
	tmpDir, err := root.FixedDir(tmpDirName)
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
	id, err := bf.Ingest(root.Dir)
	if err != nil {
		t.Fatal(err)
	}
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

// directory materialize tests
func TestMaterializeDir(t *testing.T) {
	root, err := temp.TempDirDefault()
	if err != nil {
		t.Fatal(err)
	}
	tmpDirName := "tmp"
	tmpDir, err := root.FixedDir(tmpDirName)
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
	id, err := bf.Ingest(root.Dir)
	if err != nil {
		t.Fatal(err)
	}

	newTemp, err := temp.TempDirDefault()
	if err != nil {
		t.Fatal(newTemp)
	}
	_, err = bf.Checkout(id)
	if err != nil {
		t.Fatal(err)
	}
}
