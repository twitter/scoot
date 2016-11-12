package snapshots

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
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

func assertFileContains(path, contents, msg string, t *testing.T) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(msg + ", readfile: " + err.Error())
	}
	if string(b) != contents {
		t.Fatal(msg + ", contents: " + string(b))
	}
}

func assertDirEntries(path string, count int, msg string, t *testing.T) {
	fi, err := ioutil.ReadDir(path)
	if err != nil {
		t.Fatal(msg + ", readdir: " + err.Error())
	}
	if len(fi) != count {
		log.Print("entries: ", fi) //FIXME: tmp dbg.
		t.Fatal(msg + ", entrycount: " + strconv.Itoa(len(fi)))
	}

}

func TestTempFiler(t *testing.T) {
	// Initialize snapshots tmpdir and filer.
	snapTmp, _ := temp.NewTempDir(os.TempDir(), "TestTempFiler_filer")
	filer := MakeTempFiler(snapTmp)

	// Populate the paths we want to ingest.
	localtmp, _ := temp.NewTempDir(os.TempDir(), "TestTempFiler_localpath")
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
	assertDirEntries(co1.Path(), 1, "co1", t)

	// Read single file from first checkout.
	assertFileContains(filepath.Join(co1.Path(), filepath.Base(localfile1)), "bar1", "co1", t)

	// Make sure second checkout contains both files.
	assertDirEntries(co2.Path(), 2, "co2", t)

	// Read first file from the second checkout subdirectory.
	p := filepath.Join(co2.Path(), filepath.Base(localfile1))
	assertFileContains(p, "bar1", "co2", t)

	// Read second file from the second checkout subdirectory.
	p = filepath.Join(co2.Path(), filepath.Base(localfile2))
	assertFileContains(p, "bar2", "co2", t)

	//TODO? test that checkouts can modify files without affecting stored snapshots.
}
