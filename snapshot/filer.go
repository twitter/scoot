package snapshot

import (
	"os/exec"
	"time"

	log "github.com/sirupsen/logrus"
)

// A Snapshot is a low-level interface offering per-file access to data in a Snapshot.
// This is useful for tools that want one file at a time, or for ScootFS to offer the data.
// Many tools want a higher-level construct: a Filer.

// A Filer lets clients deal with Snapshots as files in the local filesystem.
type Filer interface {
	Checkouter
	Ingester
	Updater
}

// Checkouter allows reading a Snapshot into the local filesystem.
type Checkouter interface {
	// Checkout checks out the Snapshot identified by id, or an error if it fails.
	Checkout(id string) (Checkout, error)

	// Create checkout in a caller controlled dir.
	CheckoutAt(id string, dir string) (Checkout, error)
}

// Checkout represents one checkout of a Snapshot.
// A Checkout is a copy of a Snapshot that lives in the local filesystem at a path.
type Checkout interface {
	// Path in the local filesystem to the Checkout
	Path() string

	// ID of the checked-out Snapshot
	ID() string

	// Releases this Checkout, allowing the Checkouter to clean/recycle this checkout.
	// After Release(), the client may not look at files under Path().
	Release() error
}

// Ingester creates a Snapshot from a path in the local filesystem.
type Ingester interface {
	// Takes an absolute path on the local filesystem.
	// The contents of path will be stored in a snapshot which may then be checked out by id.
	Ingest(path string) (id string, err error)

	// Takes a mapping of source paths to be copied into corresponding destination directories.
	// Source paths are absolute, and destination directories are relative to Checkout root.
	IngestMap(srcToDest map[string]string) (id string, err error)
}

const NoDuration time.Duration = time.Duration(0)

// Updater allows Filers to have a means to manage updates on the underlying resources
type Updater interface {
	// Trigger an update on the underlying resource
	Update() error

	// Get the configured update frequency from the Updater.
	// This lets us use the high-level interface to control update concurrency.
	UpdateInterval() time.Duration
}

//TODO: this is temporary until we finalize snapshot.DB and gitDB.
func NewDBAdapter(db DB) Filer {
	return &dbAdapter{db: db}
}

type dbAdapter struct {
	db DB
}

func (dba *dbAdapter) Checkout(id string) (Checkout, error) {
	if dir, err := dba.db.Checkout(ID(id)); err != nil {
		return nil, err
	} else {
		return &dbCheckout{db: dba.db, dir: dir, id: id}, nil
	}
}

func (dba *dbAdapter) CheckoutAt(id string, dir string) (Checkout, error) {
	if co, err := dba.Checkout(id); err != nil {
		return nil, err
	} else if err := exec.Command("cp", "-r", co.Path()+"/.", dir).Run(); err != nil {
		return nil, err
	} else {
		return &dbCheckout{db: dba.db, dir: dir, id: id}, nil
	}
}

func (dba *dbAdapter) Ingest(path string) (id string, err error) {
	if ident, err := dba.db.IngestDir(path); err != nil {
		return "", err
	} else {
		return string(ident), nil
	}
}

func (dba *dbAdapter) IngestMap(srcToDest map[string]string) (id string, err error) {
	log.Error("Not implemented")
	return "", nil
}

func (dba *dbAdapter) Update() error {
	return dba.db.Update()
}

func (dba *dbAdapter) UpdateInterval() time.Duration {
	return dba.db.UpdateInterval()
}

type dbCheckout struct {
	db  DB
	dir string
	id  string
}

func (dbc *dbCheckout) Path() string {
	return dbc.dir
}

func (dbc *dbCheckout) ID() string {
	return dbc.id
}

func (dbc *dbCheckout) Release() error {
	return dbc.db.ReleaseCheckout(dbc.dir)
}
