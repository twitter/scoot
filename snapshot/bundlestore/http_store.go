package bundlestore

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"github.com/scootdev/scoot/common/log"
	"net/http"
	"os"
	"strings"
	"time"
)

func MakeHTTPStore(rootURI string) Store {
	if !strings.HasSuffix(rootURI, "/") {
		rootURI = rootURI + "/"
	}
	client := &http.Client{Timeout: 30 * time.Second}
	return &httpStore{rootURI, client}
}

type httpStore struct {
	rootURI string
	client  *http.Client
}

func (s *httpStore) OpenForRead(name string) (io.ReadCloser, error) {
	uri := s.rootURI + name
	log.Infof("Fetching %s", uri)
	resp, err := s.client.Get(uri)
	if err != nil {
		log.Infof("Fetched w/error: %s %v", uri, err)
		return nil, err
	}
	log.Infof("Fetch result %s %v", uri, resp.StatusCode)

	if resp.StatusCode == http.StatusOK {
		return resp.Body, nil
	}

	resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, os.ErrNotExist
	} else if resp.StatusCode == http.StatusBadRequest {
		return nil, os.ErrInvalid
	}
	return nil, fmt.Errorf("could not open: %+v", resp)
}

func (s *httpStore) Exists(name string) (bool, error) {
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

func (s *httpStore) Write(name string, data io.Reader) error {
	if strings.Contains(name, "/") {
		return errors.New("'/' not allowed in name when writing bundles.")
	}
	uri := s.rootURI + name
	log.Infof("Posting %s", uri)
	resp, err := s.client.Post(uri, "text/plain", data)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			data, _ := ioutil.ReadAll(resp.Body)
			return errors.New(resp.Status + ": " + string(data))
		}
	}
	log.Infof("Posted %s, err: %v", uri, err)
	return err
}
