package store

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

// Implements Store with no actions being taken on any methods.
type NoopStore struct {
}

func (f *NoopStore) Exists(name string) (bool, error) {
	log.Infof("Noop Exists returning false")
	return false, nil
}

func (f *NoopStore) OpenForRead(name string) (*Resource, error) {
	log.Infof("Noop OpenForRead returning nil and noop retrieve error.")
	return nil, fmt.Errorf("Retreiving %s from Noop store. (No values stored in Noop store.)", name)
}

func (f *NoopStore) Root() string { return "" }

func (f *NoopStore) Write(name string, resource *Resource) error {
	log.Infof("Noop Write returning nil.")
	return nil
}
