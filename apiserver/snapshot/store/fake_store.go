package store

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"
)

// Implements Store. FakeStore just keeps references to data that would be stored
type FakeStore struct {
	Files sync.Map // map[string][]byte
	TTL   *TTLValue
}

func (f *FakeStore) Exists(name string) (bool, error) {
	if _, ok := f.Files.Load(name); !ok {
		return false, nil
	}
	return true, nil
}

func (f *FakeStore) OpenForRead(name string) (*Resource, error) {
	v, ok := f.Files.Load(name)
	if !ok {
		return nil, errors.New("Doesn't exist :" + name)
	}
	b, ok := v.([]byte)
	if !ok {
		return nil, errors.New("Couldn't read data as []byte")
	}
	rc := ioutil.NopCloser(bytes.NewBuffer(b))

	return NewResource(rc, int64(len(b)), f.TTL), nil
}

func (f *FakeStore) Root() string { return "" }

func (f *FakeStore) Write(name string, resource *Resource) error {
	if (f.TTL == nil) != (resource.TTLValue == nil) || (resource.TTLValue != nil && (f.TTL.TTLKey != resource.TTLValue.TTLKey || f.TTL.TTL.Sub(resource.TTLValue.TTL) != 0)) {
		return fmt.Errorf("TTL mismatch: expected: %v, got: %v", f.TTL, resource.TTLValue)
	}

	b, err := ioutil.ReadAll(resource)
	if err != nil {
		return err
	} else {
		f.Files.Store(name, b)
		return nil
	}
}
