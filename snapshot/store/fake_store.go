package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
)

// Implements Store. FakeStore just keeps references to data that would be stored
type FakeStore struct {
	Files map[string][]byte
	TTL   *TTLValue
}

func (f *FakeStore) Exists(name string) (bool, error) {
	if _, ok := f.Files[name]; !ok {
		return false, nil
	}
	return true, nil
}

func (f *FakeStore) OpenForRead(name string) (io.ReadCloser, error) {
	if ok, _ := f.Exists(name); !ok {
		return nil, errors.New("Doesn't exist :" + name)
	}
	return ioutil.NopCloser(bytes.NewBuffer(f.Files[name])), nil
}

func (f *FakeStore) Root() string { return "" }

func (f *FakeStore) Write(name string, data io.Reader, ttl *TTLValue) error {
	if (f.TTL == nil) != (ttl == nil) || (ttl != nil && (f.TTL.TTLKey != ttl.TTLKey || f.TTL.TTL.Sub(ttl.TTL) != 0)) {
		return fmt.Errorf("TTL mismatch: expected: %v, got: %v", f.TTL, ttl)
	}

	// Initialize map on first entry
	if f.Files == nil {
		f.Files = make(map[string][]byte)
	}

	b, err := ioutil.ReadAll(data)
	if err != nil {
		return err
	} else {
		f.Files[name] = b
		return nil
	}
}
