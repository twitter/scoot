package snapshots

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/snapshot"
)

// Make an invalid Filer
func MakeInvalidFiler() snapshot.Filer {
	return MakeFilerFacade(MakeInvalidCheckouter(), MakeNoopIngester(), MakeNoopUpdater())
}
func MakeNoopFiler(path string) snapshot.Filer {
	return MakeFilerFacade(MakeNoopCheckouter(path), MakeNoopIngester(), MakeNoopUpdater())
}

// Make invalid filer with an updater
func MakeInvalidFilerUpdater(updater snapshot.Updater) snapshot.Filer {
	return MakeFilerFacade(MakeInvalidCheckouter(), MakeNoopIngester(), updater)
}

// FilerFacade creates a Filer from a Checkouter and Ingester
type FilerFacade struct {
	snapshot.Checkouter
	snapshot.Ingester
	snapshot.Updater
}

// Make a Filer from a Checkouter and Ingester
func MakeFilerFacade(
	checkouter snapshot.Checkouter,
	ingester snapshot.Ingester,
	updater snapshot.Updater) *FilerFacade {
	return &FilerFacade{checkouter, ingester, updater}
}

// Make a Filer that can Checkout() but does a noop Ingest().
func MakeTempCheckouterFiler(tmp *temp.TempDir) snapshot.Filer {
	return MakeFilerFacade(MakeTempCheckouter(tmp), MakeNoopIngester(), MakeNoopUpdater())
}

// Creates a filer that copies ingested paths in and then back out for checkouts.
func MakeTempFiler(tmp *temp.TempDir) snapshot.Filer {
	return &tempFiler{tmp: tmp, snapshots: make(map[string]string)}
}

type tempFiler struct {
	tmp       *temp.TempDir
	snapshots map[string]string
}

func (t *tempFiler) Ingest(path string) (id string, err error) {
	return t.IngestMap(map[string]string{path: ""})
}

func (t *tempFiler) IngestMap(srcToDest map[string]string) (id string, err error) {
	var s *temp.TempDir
	s, err = t.tmp.TempDir("snapshot-")
	if err != nil {
		return "", err
	}
	for src, dest := range srcToDest {
		// absDest is a parent directory in which we place the contents of src.
		absDest := filepath.Join(s.Dir, dest)

		slashDot := ""
		if fi, err := os.Stat(src); err == nil && fi.IsDir() {
			// If src is a dir, we append a slash dot to copy contents rather than the dir itself.
			slashDot = "/."
			err = os.MkdirAll(absDest, os.ModePerm)
		} else {
			// If src is a file, we treat the base of absDest as a file instead of a directory.
			err = os.MkdirAll(filepath.Dir(absDest), os.ModePerm)
		}
		if err != nil {
			return
		}

		err = exec.Command("sh", "-c", fmt.Sprintf("cp -r %s%s %s", src, slashDot, absDest)).Run()
		if err != nil {
			return
		}
	}

	id = strconv.Itoa(len(t.snapshots))
	t.snapshots[id] = s.Dir
	return
}

func (t *tempFiler) Checkout(id string) (snapshot.Checkout, error) {
	dir, err := t.tmp.TempDir("checkout-" + id + "__")
	if err != nil {
		return nil, err
	}
	co, err := t.CheckoutAt(id, dir.Dir)
	if err != nil {
		os.RemoveAll(dir.Dir)
		return nil, err
	}
	return co, nil
}

func (t *tempFiler) CheckoutAt(id string, dir string) (snapshot.Checkout, error) {
	snap, ok := t.snapshots[id]
	if !ok {
		return nil, errors.New("No snapshot with id: " + id)
	}

	// Copy contents of snapshot dir to the output dir using cp '.' terminator syntax (incompatible with path/filepath).
	if err := exec.Command("cp", "-rf", snap+"/.", dir).Run(); err != nil {
		return nil, err
	}
	return &staticCheckout{
		path: dir,
		id:   id,
	}, nil
}

func (t *tempFiler) Update() error { return nil }

func (t *tempFiler) UpdateInterval() time.Duration { return snapshot.NoDuration }

// Make an Ingester that does nothing
func MakeNoopIngester() *NoopIngester {
	return &NoopIngester{}
}

// Ingester that does nothing.
type NoopIngester struct{}

func (n *NoopIngester) Ingest(string) (string, error) {
	return "", nil
}
func (n *NoopIngester) IngestMap(map[string]string) (string, error) {
	return "", nil
}

// Make an Updater that does nothing
func MakeNoopUpdater() *NoopUpdater {
	return &NoopUpdater{}
}

type NoopUpdater struct{}

func (n *NoopUpdater) Update() error { return nil }

func (n *NoopUpdater) UpdateInterval() time.Duration { return snapshot.NoDuration }
