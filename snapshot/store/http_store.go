package store

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
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
		log.Errorf("Retrying after failed attempt: %+v", e)
	}
	return client
}

func MakeHTTPStore(rootURI string) Store {
	return MakeCustomHTTPStore(rootURI, MakePesterClient(), &TTLConfig{TTL: DefaultTTL, TTLKey: DefaultTTLKey, TTLFormat: DefaultTTLFormat})
}

func MakeCustomHTTPStore(rootURI string, client Client, ttlc *TTLConfig) Store {
	if !strings.HasSuffix(rootURI, "/") {
		rootURI = rootURI + "/"
	}
	if ttlc == nil {
		ttlc = &TTLConfig{TTLKey: DefaultTTLKey, TTLFormat: DefaultTTLFormat}
	}
	log.Infof("Making new HTTP Store with root URI: %s", rootURI)
	return &httpStore{rootURI, client, *ttlc}
}

type Client interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

type httpStore struct {
	rootURI string
	client  Client
	ttlc    TTLConfig
}

func (s *httpStore) getTTLValue(resp *http.Response) *TTLValue {
	expire := resp.Header.Get(s.ttlc.TTLKey)
	ttl, err := time.Parse(s.ttlc.TTLFormat, expire)
	if err != nil {
		return nil
	}
	return &TTLValue{TTL: ttl, TTLKey: s.ttlc.TTLKey}
}

func (s *httpStore) OpenForRead(name string) (*Resource, error) {
	return s.openForRead(name, false)
}

func (s *httpStore) openForRead(name string, existCheck bool) (*Resource, error) {
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
		ttlv := s.getTTLValue(resp)
		var rc io.ReadCloser
		if existCheck {
			rc = ioutil.NopCloser(resp.Body)
		} else {
			rc = resp.Body
		}
		return NewResource(rc, resp.ContentLength, ttlv), nil
	}
	log.Errorf("%s response status error: %s %v", label, uri, resp.Status)
	if !existCheck {
		resp.Body.Close()
	}
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
		log.Errorf("Exists error: %s %v", name, err)
		return false, err
	}
	log.Infof("Exists ok: %s", name)
	r.Close()
	if r.TTLValue != nil && r.TTLValue.TTL.Before(time.Now()) {
		return false, nil
	}
	return true, nil
}

func (s *httpStore) Write(name string, resource *Resource) error {
	if resource == nil {
		log.Info("Writing nil resource is a no op.")
		return nil
	}
	if strings.Contains(name, "/") {
		log.Errorf("Write error: %s '/' not allowed", name)
		return errors.New("'/' not allowed in name when writing bundles.")
	}
	uri := s.rootURI + name

	post := func() (*http.Response, error) {
		req, err := http.NewRequest("POST", uri, resource)
		if err != nil {
			return nil, err
		}
		req.ContentLength = resource.Length
		req.Header.Set("Content-Type", "text/plain")
		ttl := resource.TTLValue
		if ttl == nil {
			ttl = &TTLValue{TTL: time.Now().Add(s.ttlc.TTL), TTLKey: s.ttlc.TTLKey}
		}
		if ttl.TTLKey != "" {
			req.Header[ttl.TTLKey] = []string{ttl.TTL.Format(s.ttlc.TTLFormat)}
		}
		log.Infof("Writing %s: length: %d header: %v", uri, req.ContentLength, req.Header)
		return s.client.Do(req)
	}

	resp, err := post()
	if err != nil {
		log.Errorf("Write error: %s %v", uri, err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Errorf("Write response status error: %s -- %s", uri, resp.Status)
			return errors.New(resp.Status)
		}
	}
	return err
}

func (s *httpStore) Root() string {
	return s.rootURI
}
