package bundlestore

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

func MakeHTTPStore(rootURI string) *HTTPStore {
	client := &http.Client{Timeout: 30 * time.Second}
	return &HTTPStore{rootURI, client}
}

type HTTPStore struct {
	rootURI string
	client  *http.Client
}

func (s *HTTPStore) OpenForRead(name string) (io.ReadCloser, error) {
	uri := s.rootURI + name
	log.Printf("Fetching %s", uri)
	resp, err := s.client.Get(uri)
	if err != nil {
		log.Printf("Fetched %s %v", uri, err)
		return nil, err
	}
	log.Printf("Fetched %s %v", uri, resp.StatusCode)

	if resp.StatusCode == http.StatusOK {
		return resp.Body, nil
	}

	resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, os.ErrNotExist
	}

	return nil, fmt.Errorf("could not open: %+v", resp)
}

func (s *HTTPStore) Exists(name string) (bool, error) {
	r, err := s.OpenForRead(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	r.Close()
	return true, nil
}

func (s *HTTPStore) Write(name string, data io.Reader) error {
	uri := s.rootURI + name
	log.Printf("Posting %s", uri)
	_, err := s.client.Post(uri, "text/plain", data)
	log.Printf("Posted %s, err: %v", uri, err)
	return err
}
