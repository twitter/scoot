package snapshots

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot"
)

type wgIniter struct {
	wg sync.WaitGroup
}

func (i *wgIniter) Init() error {
	i.wg.Wait()
	return nil
}

func TestIniting(t *testing.T) {
	i := &wgIniter{}
	i.wg.Add(1)

	c := MakeInitingCheckouter("/fake/path", i)
	resultCh := make(chan snapshot.Checkout)
	go func() {
		result, _ := c.Checkout("foo")
		resultCh <- result
	}()

	select {
	case <-resultCh:
		t.Fatalf("should't be able to checkout until init is done")
	default:
	}

	i.wg.Done()
	checkout := <-resultCh
	if checkout.Path() != "/fake/path" {
		t.Fatalf("surprising path: %q", checkout.Path())
	}
}

func TestTempFiler(t *testing.T) {
	// Initialize snapshots tmpdir and filer.
	snapTmp, _ := temp.NewTempDir(os.TempDir(), "snap")
	filer := MakeTempFiler(snapTmp)

	// Populate the paths we want to ingest.
	localtmp, _ := temp.NewTempDir(os.TempDir(), "localpath")
	localfile1 := filepath.Join(localtmp.Dir, "foo1")
	localfile2 := filepath.Join(localtmp.Dir, "foo2")
	ioutil.WriteFile(localfile1, []byte("bar1"), os.ModePerm)
	ioutil.WriteFile(localfile2, []byte("bar2"), os.ModePerm)

	// Ingest single file first, then a directory with two files.
	var err error
	var id1, id2 string
	id1, err = filer.Ingest(localfile1)
	if err != nil {
		t.Fatalf("ingest single file: %v", err)
	}
	id2, err = filer.Ingest(localtmp.Dir)
	if err != nil {
		t.Fatalf("ingest dir: %v", err)
	}

	// Retrieve checkouts for the snapshot ids produced above.
	var co1, co2 snapshot.Checkout
	co1, err = filer.Checkout(id1)
	if err != nil {
		t.Fatalf("checkout single file: %v", err)
	}
	co2, err = filer.Checkout(id2)
	if err != nil {
		t.Fatalf("checkout dir: %v", err)
	}
	defer co1.Release()
	defer co2.Release()

	// Make sure first checkout only contains a single file.
	var fi []os.FileInfo
	fi, err = ioutil.ReadDir(co1.Path())
	if err != nil {
		t.Fatalf("read single file root: %v", err)
	}
	if len(fi) != 1 {
		t.Fatalf("see exactly one file in root: %v", len(fi))
	}

	// Read single file from first checkout.
	var b []byte
	b, err = ioutil.ReadFile(filepath.Join(co1.Path(), filepath.Base(localfile1)))
	if err != nil {
		t.Fatalf("read single file: %v", err)
	}
	if string(b) != "bar1" {
		t.Fatalf("contents of single file: %v", string(b))
	}

	// Make sure second checkout contains a single entry.
	fi, err = ioutil.ReadDir(co2.Path())
	if err != nil {
		t.Fatalf("read dir root: %v", err)
	}
	if len(fi) != 1 {
		t.Fatalf("see exactly one dir in root: %v", len(fi))
	}

	// Read first file from the second checkout subdirectory.
	b, err = ioutil.ReadFile(filepath.Join(co2.Path(), filepath.Base(localtmp.Dir), filepath.Base(localfile1)))
	if err != nil {
		t.Fatalf("read first file in dir: %v", err)
	}
	if string(b) != "bar1" {
		t.Fatalf("contents of dir file1: %v", err)
	}

	// Read second file from the second checkout subdirectory.
	b, err = ioutil.ReadFile(filepath.Join(co2.Path(), filepath.Base(localtmp.Dir), filepath.Base(localfile2)))
	if err != nil {
		t.Fatalf("read second file in dir: %v", err)
	}
	if string(b) != "bar2" {
		t.Fatalf("contents of dir file2: %v", err)
	}

	//TODO? test that checkouts can modify files without affecting stored snapshots.
}
