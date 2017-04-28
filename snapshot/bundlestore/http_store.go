package bundlestore

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
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
		log.Infof("Fetch error: %s %v", uri, err)
		return nil, err
	}
	log.Infof("Fetch result %s %v", uri, resp.StatusCode)

	if resp.StatusCode == http.StatusOK {
		return resp.Body, nil
	}

	resp.Body.Close()
	log.Infof("Fetch response status error: %s %v", uri, resp.Status)
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
		log.Infof("Exists error: %s %v", name, err)
		return false, err
	}
	r.Close()
	return true, nil
}

func (s *httpStore) Write(name string, data io.Reader, ttl *TTLValue) error {
	if strings.Contains(name, "/") {
		log.Infof("Write error: %s '/' not allowed", name)
		return errors.New("'/' not allowed in name when writing bundles.")
	}
	uri := s.rootURI + name
	log.Infof("Writing %s", uri)

	post := func() (*http.Response, error) {
		req, err := http.NewRequest("POST", uri, data)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "text/plain")
		if ttl == nil {
			ttl = &TTLValue{time.Now().Add(DefaultTTL), DefaultTTLKey}
		}
		if ttl.TTLKey != "" {
			req.Header[ttl.TTLKey] = []string{ttl.TTL.Format(time.RFC1123)}
		}
		log.Infof("Write header: %s %v", uri, req.Header)
		return s.client.Do(req)
	}

	resp, err := post()
	if err != nil {
		log.Infof("Write error: %s %v", uri, err)
	} else {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			data, _ := ioutil.ReadAll(resp.Body)
			log.Infof("Write response status error: %s %v -- %s", uri, resp.Status, string(data))
			return errors.New(resp.Status + ": " + string(data))
		}
	}
	return err
}
