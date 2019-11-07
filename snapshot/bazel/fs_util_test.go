// +build integration

package bazel

import (
	"os/exec"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/runner/execer"
	"github.com/twitter/scoot/runner/execer/execers"
)

// uses test vars and setup/teardown defined in filer_test.go

func init() {
	log.AddHook(hooks.NewContextHook())
}

func makeTestingFiler() *BzFiler {
	bf, err := MakeBzFiler(tmpTest, noopRes)
	if err != nil {
		log.Fatal(err)
	}
	return bf
}

// directory save tests

func TestSaveEmptyDir(t *testing.T) {
	root, err := tmpTest.TempDir("")
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
	root, err := tmpTest.TempDir("")
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
	sha, size, err := bazel.GetShaAndSize(id)
	if err != nil {
		t.Fatal(err)
	}
	if sha == bazel.EmptySha {
		t.Fatalf("Expected sha to not be %s. ID: %s", bazel.EmptySha, id)
	}
	if size <= bazel.EmptySize {
		t.Fatalf("Expected size to be >%d, ID: %s", bazel.EmptySize, id)
	}
}

// file save tests

func TestSaveEmptyFile(t *testing.T) {
	root, err := tmpTest.TempDir("")
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
	root, err := tmpTest.TempDir("")
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
	sha, size, err := bazel.GetShaAndSize(id)
	if err != nil {
		t.Fatal(err)
	}
	if sha == bazel.EmptySha {
		t.Fatalf("Expected sha to not be %s. ID: %s", bazel.EmptySha, id)
	}
	if size <= bazel.EmptySize {
		t.Fatalf("Expected size to be >%d, ID: %s", bazel.EmptySize, id)
	}
}

// directory materialize test

func TestMaterializeDir(t *testing.T) {
	root, err := tmpTest.TempDir("")
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
	output, err := exec.Command("diff", "-r", co.Path(), tmpDir.Dir).Output()
	if string(output) != "" {
		t.Fatalf("Expected %s and %s to be equivalent, instead received \"%s\" from command", co.Path(), tmpDir.Dir, string(output))
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestMaterializeEmptyDir(t *testing.T) {
	root, err := tmpTest.TempDir("")
	if err != nil {
		t.Fatal(err)
	}
	tmpDirPrefix := "tmp"
	tmpDir, err := root.TempDir(tmpDirPrefix)
	if err != nil {
		t.Fatal(err)
	}

	id := bazel.SnapshotID(bazel.EmptySha, bazel.EmptySize)
	bf := makeTestingFiler()

	co, err := bf.Checkout(id)
	if err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command("diff", "-r", co.Path(), tmpDir.Dir).Output()
	if string(output) != "" {
		t.Fatalf("Expected %s and %s to be equivalent, instead received \"%s\" from command", co.Path(), tmpDir.Dir, string(output))
	}
	if err != nil {
		t.Fatal(err)
	}
}

// test cancellation functionality
func TestCancelOperation(t *testing.T) {
	_, err := tmpTest.TempDir("")
	if err != nil {
		t.Fatal(err)
	}

	bf := makeTestingFiler()
	// Override bf's bzCommand's execer with a sim execer
	bc := bf.tree.(*bzCommand)
	bc.execer = execers.NewSimExecer()

	cmd := execer.Command{
		Argv: []string{"pause", "complete 0"},
	}
	doneCh := make(chan execer.ProcessStatus)

	go func() {
		doneCh <- bc.exec(cmd)
	}()

	// simplest way to ensure cancel gets run after exec actually starts
	go func() {
		for {
			bc.cancel()
			time.Sleep(1 * time.Millisecond)
		}
	}()

	select {
	case st := <-doneCh:
		if st.State != execer.FAILED {
			t.Fatalf("Expected state after Abort: %s, got: %s", execer.FAILED, st.State)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Hit unexpected timeout waiting for bzCommand exec to finish/abort")
	}
}

// test timeout functionality
func TestTimeoutCommand(t *testing.T) {
	_, err := tmpTest.TempDir("")
	if err != nil {
		t.Fatal(err)
	}

	bf := makeTestingFiler()
	bc := bf.tree.(*bzCommand)
	// Override timeout on bzCommand
	bc.timeout = 1 * time.Millisecond
	// Override bf's bzCommand's execer with a sim execer
	bc.execer = execers.NewSimExecer()

	cmd := execer.Command{
		Argv: []string{"pause", "complete 0"},
	}
	doneCh := make(chan execer.ProcessStatus)

	go func() {
		doneCh <- bc.exec(cmd)
	}()

	select {
	case st := <-doneCh:
		if st.State != execer.FAILED {
			t.Fatalf("Expected state after Abort: %s, got: %s", execer.FAILED, st.State)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Hit unexpected timeout waiting for bzCommand exec to finish/abort")
	}
}
