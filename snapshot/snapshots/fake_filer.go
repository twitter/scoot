package snapshots

import (
	"errors"
	"os"
	"os/exec"
	"strconv"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot"
)

// Creates a filer that copies ingested paths in and then back out for checkouts.
func MakeTempFiler(tmp *temp.TempDir) snapshot.Filer {
	return &tempFiler{tmp: tmp, snapshots: make(map[string]string)}
	//return &tempFiler{tempIngester: MakeTempIngester(tmp).(*tempIngester)}
}

type tempFiler struct {
	tmp       *temp.TempDir
	snapshots map[string]string
}

func (t *tempFiler) Ingest(path string) (id string, err error) {
	_, err = os.Stat(path)
	if err != nil {
		return "", err
	}

	var s *temp.TempDir
	s, err = t.tmp.TempDir("snapshot-")
	if err != nil {
		return "", err
	}
	err = exec.Command("cp", "-rf", path, s.Dir).Run()
	if err != nil {
		return "", err
	}

	id = strconv.Itoa(len(t.snapshots))
	t.snapshots[id] = s.Dir
	return
}

func (t *tempFiler) Checkout(id string) (snapshot.Checkout, error) {
	snap, ok := t.snapshots[id]
	if !ok {
		return nil, errors.New("No snapshot with id: " + id)
	}

	var err error
	var co *temp.TempDir
	co, err = t.tmp.TempDir("checkout-")
	if err != nil {
		return nil, err
	}
	// Copy contents of snapshot dir to the output dir using cp '.' terminator syntax (incompatible with path/filepath).
	err = exec.Command("cp", "-rf", snap+"/.", co.Dir).Run()
	if err != nil {
		return nil, err
	}

	return &staticCheckout{
		path: co.Dir,
		id:   id,
	}, nil
}
