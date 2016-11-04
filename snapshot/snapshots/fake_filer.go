package snapshots

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot"
)

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
		absDest := filepath.Join(s.Dir, dest)
		if strings.Contains(absDest, "*") {
			// If wildcard is present, treat destination as a parent directory.
			err = os.MkdirAll(absDest, os.ModePerm)
		} else {
			// If no wildcard, treat destination as dir/base.
			err = os.MkdirAll(filepath.Dir(absDest), os.ModePerm)
		}
		if err != nil {
			return
		}

		err = exec.Command("sh", "-c", fmt.Sprintf("cp -r %s %s", src, absDest)).Run()
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

// Ingester that does nothing.
type noopIngester struct{}

func (n *noopIngester) Ingest(string) (string, error) {
	return "", nil
}
func (n *noopIngester) IngestMap(map[string]string) (string, error) {
	return "", nil
}

// Make in invalid Filer
func MakeInvalidFiler() snapshot.Filer {
	return &noopFiler{}
}

type noopFiler struct {
	noopCheckouter
	noopIngester
}

// Make a Filer that can Checkout() but does a noop Ingest().
func MakeTempCheckouterFiler(tmp *temp.TempDir) snapshot.Filer {
	return &tempCheckouterFiler{Checkouter: MakeTempCheckouter(tmp)}
}

type tempCheckouterFiler struct {
	snapshot.Checkouter
	noopIngester
}
