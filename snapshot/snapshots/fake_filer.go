package snapshots

import (
	"errors"
	"os"
	"os/exec"
	"strconv"
	"sync"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot"
)

func MakeInvalidCheckouter() snapshot.Checkouter {
	return &noopCheckouter{path: "/path/is/invalid"}
}

type noopCheckouter struct {
	path string
}

func (c *noopCheckouter) Checkout(id string) (snapshot.Checkout, error) {
	return &staticCheckout{
		path: c.path,
		id:   id,
	}, nil
}

// MakeTempCheckouter creates a new Checkouter that always checks out by creating a new, empty temp dir
func MakeTempCheckouter(tmp *temp.TempDir) snapshot.Checkouter {
	return &tempCheckouter{tmp: tmp}
}

type tempCheckouter struct {
	tmp *temp.TempDir
}

func (c *tempCheckouter) Checkout(id string) (snapshot.Checkout, error) {
	t, err := c.tmp.TempDir("checkout-")
	if err != nil {
		return nil, err
	}
	return &staticCheckout{
		path: t.Dir,
		id:   id,
	}, nil
}

type staticCheckout struct {
	path string
	id   string
}

func (c *staticCheckout) Path() string {
	return c.path
}

func (c *staticCheckout) ID() string {
	return c.id
}

func (c *staticCheckout) Release() error {
	return nil
}

// Initer will do something once, e.g., clone a git repo (that might be expensive)
type Initer interface {
	Init() error
}

func MakeInitingCheckouter(path string, initer Initer) snapshot.Checkouter {
	r := &initingCheckouter{path: path}
	// Start the Initer as soon as we know we'll need to
	r.wg.Add(1)
	go func() {
		initer.Init()
		r.wg.Done()
	}()
	return r
}

// initingCheckout waits for an Initer to be done Initing before checking out.
type initingCheckouter struct {
	wg   sync.WaitGroup
	path string
}

func (c *initingCheckouter) Checkout(id string) (snapshot.Checkout, error) {
	c.wg.Wait()
	return &staticCheckout{
		path: c.path,
		id:   id,
	}, nil
}

// Creates a new Ingester that will copy ingested paths to its own tmp snapshots dir.
func MakeTempIngester(tmp *temp.TempDir) snapshot.Ingester {
	return &tempIngester{tmp: tmp, snapshots: make(map[string]string)}
}

type tempIngester struct {
	tmp       *temp.TempDir
	snapshots map[string]string
}

func (t *tempIngester) Ingest(path string) (id string, err error) {
	_, err = os.Stat(path)
	if err != nil {
		return "", nil
	}

	var snapdir *temp.TempDir
	snapdir, err = t.tmp.TempDir("snap-")
	if err != nil {
		return "", err
	}
	err = exec.Command("cp", "-rf", path, snapdir.Dir).Run()
	if err != nil {
		return "", err
	}

	id = strconv.Itoa(len(t.snapshots))
	t.snapshots[id] = snapdir.Dir
	return
}

// Creates a filer that copies ingested paths in and then back out for checkouts.
func MakeTempFiler(tmp *temp.TempDir) snapshot.Filer {
	return &tempFiler{tempIngester: MakeTempIngester(tmp).(*tempIngester)}
}

type tempFiler struct {
	*tempIngester
}

func (t *tempFiler) Checkout(id string) (snapshot.Checkout, error) {
	snap, ok := t.tempIngester.snapshots[id]
	if !ok {
		return nil, errors.New("No snapshot with id: " + id)
	}

	var err error
	var outdir *temp.TempDir
	outdir, err = t.tmp.TempDir("co-")
	if err != nil {
		return nil, err
	}
	// Copy contents of snapshot dir to the output dir using cp '.' terminator syntax (incompatible with path/filepath).
	err = exec.Command("cp", "-rf", snap+"/.", outdir.Dir).Run()
	if err != nil {
		return nil, err
	}

	return &staticCheckout{
		path: outdir.Dir,
		id:   id,
	}, nil
}
