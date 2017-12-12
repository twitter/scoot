package bazel

import (
	"fmt"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/os/temp"
)

func init() {
	log.AddHook(hooks.NewContextHook())
}

func makeTestingFiler(tmpDir *temp.TempDir) *bzFiler {
	localStorePath := func(bc *bzCommand) {
		bc.localStorePath = tmpDir.Dir
	}
	root := func(bc *bzCommand) {
		bc.root = tmpDir.Dir
	}
	bf := MakeBzFilerWithOptions(localStorePath, root)
	return bf
}

func TestSaveEmptyDir(t *testing.T) {
	tmpDir, err := temp.TempDirDefault()
	if err != nil {
		t.Fatal(err)
	}
	bf := makeTestingFiler(tmpDir)
	id, err := bf.Ingest(".")
	if err != nil {
		t.Fatal(err)
	}
	expectedID := fmt.Sprintf("%s-%s-%d", bzSnapshotIdPrefix, emptySha, int64(0))
	if id != expectedID {
		t.Fatalf("Expected id to be %s, was %s", id, expectedID)
	}
}

func TestSaveEmptyFile(t *testing.T) {
	tmpDir, err := temp.TempDirDefault()
	if err != nil {
		t.Fatal(err)
	}
	f, err := tmpDir.TempFile("empty")
	if err != nil {
		t.Fatal(err)
	}
	bf := makeTestingFiler(tmpDir)
	id, err := bf.Ingest(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if id != emptyID {
		t.Fatalf("Expected id to be %s, was %s", id, emptyID)
	}
}

func TestSaveFile(t *testing.T) {
	tmpDir, err := temp.TempDirDefault()
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
	bf := makeTestingFiler(tmpDir)
	id, err := bf.Ingest(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if id == emptyID {
		t.Fatalf("Expected id to not be %s, was %s", id, emptyID)
	}
}
