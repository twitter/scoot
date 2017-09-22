package bundlestore

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sethgrid/pester"
	log "github.com/sirupsen/logrus"
)

const DefaultHttpTries = 7 // ~2min total of trying with exponential backoff (0 and 1 both mean 1 try total)

func MakePesterClient() *pester.Client {
	client := pester.New()
	client.Backoff = pester.ExponentialBackoff
	client.MaxRetries = DefaultHttpTries
	client.LogHook = func(e pester.ErrEntry) {
		log.Infof("Retrying after failed attempt: %+v", e)
	}
	return client
}

func MakeHTTPStore(rootURI string) Store {
	return MakeCustomHTTPStore(rootURI, MakePesterClient())
}

func MakeCustomHTTPStore(rootURI string, client Client) Store {
	if !strings.HasSuffix(rootURI, "/") {
		rootURI = rootURI + "/"
	}
	return &httpStore{rootURI, client}
}

type Client interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

type httpStore struct {
	rootURI string
	client  Client
}

func (s *httpStore) OpenForRead(name string) (io.ReadCloser, error) {
	return s.openForRead(name, false)
}

func (s *httpStore) openForRead(name string, existCheck bool) (io.ReadCloser, error) {
	label := "Read"
	if existCheck {
		label = "Exist"
	}
	uri := s.rootURI + name
	log.Infof("%sing %s", label, uri)

	var req *http.Request
	if existCheck {
		req, _ = http.NewRequest("HEAD", uri, nil)
	} else {
		req, _ = http.NewRequest("GET", uri, nil)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		if !existCheck {
			log.Infof("%s error: %s %v", label, uri, err)
		}
		return nil, err
	}

	if resp.StatusCode == http.StatusOK {
		log.Infof("%s result %s %v", label, uri, resp.StatusCode)
		return resp.Body, nil
	}
	log.Infof("%s response status error: %s %v", label, uri, resp.Status)

	resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, os.ErrNotExist
	} else if resp.StatusCode == http.StatusBadRequest {
		return nil, os.ErrInvalid
	}
	return nil, fmt.Errorf("could not open: %+v", resp)
}

func (s *httpStore) Exists(name string) (bool, error) {
	r, err := s.openForRead(name, true)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		log.Infof("Exists error: %s %v", name, err)
		return false, err
	}
	log.Infof("Exists ok: %s %v", name)
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
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Infof("Write response status error: %s %v -- %s", uri, resp.Status)
			return errors.New(resp.Status)
		}
	}
	return err
}

func (s *httpStore) Root() string {
	return s.rootURI
}
