package bundlestore

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"
)

type FakeStore struct {
	files map[string][]byte
}

func (f *FakeStore) Exists(name string) (bool, error) {
	if _, ok := f.files[name]; !ok {
		return false, nil
	}
	return true, nil
}

func (f *FakeStore) OpenForRead(name string) (io.ReadCloser, error) {
	if ok, _ := f.Exists(name); !ok {
		return nil, errors.New("Doesn't exist :" + name)
	}
	return ioutil.NopCloser(bytes.NewBuffer(f.files[name])), nil
}

func (f *FakeStore) Write(name string, data io.Reader) error {
	if data, err := ioutil.ReadAll(data); err != nil {
		return err
	} else {
		f.files[name] = data
		return nil
	}
}

//TODO: an end-end test that uses real a real store and real bundles.

func TestServer(t *testing.T) {
	// Construct server with a fake store and random port address.
	store := &FakeStore{files: map[string][]byte{}}
	listener, _ := net.Listen("tcp", "localhost:0")
	defer listener.Close()
	addr := listener.Addr().String()

	mux := http.NewServeMux()
	mux.Handle("/bundle/", MakeServer(store))
	go func() {
		http.Serve(listener, mux)
	}()

	rootUri := "http://" + addr + "/bundle/"
	client := &http.Client{Timeout: 1 * time.Second}

	// Try to write data.
	bundle1ID := "bs-0000000000000000000000000000000000000001.bundle"
	if resp, err := client.Post(rootUri+bundle1ID, "text/plain", bytes.NewBuffer([]byte("baz_data"))); err != nil {
		t.Fatalf(err.Error())
	} else {
		resp.Body.Close()
	}

	if !reflect.DeepEqual(store.files[bundle1ID], []byte("baz_data")) {
		t.Fatalf("Failed to post data.")
	}

	// Try to write data again.
	if resp, err := client.Post(rootUri+bundle1ID, "text/plain", bytes.NewBuffer([]byte("baz_data"))); err != nil {
		t.Fatalf(err.Error())
	} else {
		resp.Body.Close()
	}

	// Try to read data.
	if resp, err := client.Get(rootUri + bundle1ID); err != nil {
		t.Fatalf(err.Error())
	} else {
		defer resp.Body.Close()
		if data, err := ioutil.ReadAll(resp.Body); err != nil {
			t.Fatalf(err.Error())
		} else if !reflect.DeepEqual(data, []byte("baz_data")) {
			t.Fatalf("Failed to get data.")
		}
	}

	// Try to read non-existent data, expect NotFound.
	if resp, err := client.Get(rootUri + "bs-0000000000000000000000000000000000000000.bundle"); err != nil {
		t.Fatalf(err.Error())
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("Expected NotFound when getting non-existent data.")
		}
	}

	//
	// Repeat with HttpStore as the client.
	//

	// Try to write data.
	httpStore := MakeHTTPStore(rootUri)
	bundle2ID := "bs-0000000000000000000000000000000000000002.bundle"
	if err := httpStore.Write(bundle2ID, bytes.NewBuffer([]byte("bar_data"))); err != nil {
		t.Fatalf(err.Error())
	}

	// Check if the write succeeded.
	if ok, err := httpStore.Exists(bundle2ID); err != nil {
		t.Fatalf(err.Error())
	} else if !ok {
		t.Fatalf("Expected data to exist.")
	}

	// Try to read data.
	if reader, err := httpStore.OpenForRead(bundle2ID); err != nil {
		t.Fatalf(err.Error())
	} else if data, err := ioutil.ReadAll(reader); err != nil {
		t.Fatalf(err.Error())
	} else if !reflect.DeepEqual(data, []byte("bar_data")) {
		t.Fatalf("Failed to get matching data: " + string(data))
	}

	// Check for non-existent data.
	if ok, err := httpStore.Exists("bs-0000000000000000000000000000000000000000.bundle"); err != nil {
		t.Fatalf(err.Error())
	} else if ok {
		t.Fatalf("Expected data to not exist.")
	}

	// Check for invalid name error
	if _, err := httpStore.Exists("foo"); err == nil {
		t.Fatalf("Expected invalid input err.")
	}
}
