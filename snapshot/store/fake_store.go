package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
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

func (f *FakeStore) OpenForRead(name string) (io.ReadCloser, *TTLValue, error) {
	v, ok := f.Files.Load(name)
	if !ok {
		return nil, nil, errors.New("Doesn't exist :" + name)
	}
	b, ok := v.([]byte)
	if !ok {
		return nil, nil, errors.New("Couldn't read data as []byte")
	}

	return ioutil.NopCloser(bytes.NewBuffer(b)), f.TTL, nil
}

func (f *FakeStore) Root() string { return "" }

func (f *FakeStore) Write(name string, data io.Reader, ttl *TTLValue) error {
	if (f.TTL == nil) != (ttl == nil) || (ttl != nil && (f.TTL.TTLKey != ttl.TTLKey || f.TTL.TTL.Sub(ttl.TTL) != 0)) {
		return fmt.Errorf("TTL mismatch: expected: %v, got: %v", f.TTL, ttl)
	}

	b, err := ioutil.ReadAll(data)
	if err != nil {
		return err
	} else {
		f.Files.Store(name, b)
		return nil
	}
}
