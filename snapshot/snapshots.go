package snapshot

import (
	"fmt"
	"os/exec"
	"time"

	log "github.com/sirupsen/logrus"
)

// NewDBAdapter returns a *dbAdapter that implements the snapshot.Filer and snapshot.DB interfaces
func NewDBAdapter(db DB) *dbAdapter {
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

func (dba *dbAdapter) CancelCheckout() error {
	return nil
}

func (dba *dbAdapter) Ingest(path string) (id string, err error) {
	if ident, err := dba.db.IngestDir(path); err != nil {
		return "", err
	} else {
		return string(ident), nil
	}
}

func (dba *dbAdapter) IngestMap(srcToDest map[string]string) (string, error) {
	errMsg := "Not implemented"
	log.Error(errMsg)
	return "", fmt.Errorf(errMsg)
}

func (dba *dbAdapter) CancelIngest() error {
	return nil
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

// NewNopCheckout returns a *nopCheckout that implements snapshot.Checkout with no-ops.
func NewNopCheckout(id, dir string) *nopCheckout {
	return &nopCheckout{id: id, dir: dir}
}

type nopCheckout struct {
	dir string
	id  string
}

func (nc *nopCheckout) Path() string {
	return nc.dir
}

func (nc *nopCheckout) ID() string {
	return nc.id
}

func (nc *nopCheckout) Release() error {
	return nil
}
