package bundlestore

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/wisechengyi/scoot/common/stats"
	"github.com/wisechengyi/scoot/snapshot/store"
)

type httpServer struct {
	storeConfig *store.StoreConfig
}

func MakeHTTPServer(cfg *store.StoreConfig) *httpServer {
	return &httpServer{cfg}
}

func (s *httpServer) HandleUpload(w http.ResponseWriter, req *http.Request) {
	log.Infof("Uploading %v, %v, %v (from %v)", req.Host, req.URL, req.Header, req.RemoteAddr)
	defer s.storeConfig.Stat.Latency(stats.BundlestoreUploadLatency_ms).Time().Stop()
	s.storeConfig.Stat.Counter(stats.BundlestoreUploadCounter).Inc(1)
	bundleName := strings.TrimPrefix(req.URL.Path, "/bundle/")
	if err := checkBundleName(bundleName); err != nil {
		log.Infof("Bundlename err: %v --> StatusBadRequest (from %v)", err, req.RemoteAddr)
		http.Error(w, err.Error(), http.StatusBadRequest)
		s.storeConfig.Stat.Counter(stats.BundlestoreUploadErrCounter).Inc(1)
		return
	}
	bundleData := req.Body

	ok, err := s.storeConfig.Store.Exists(bundleName)
	if err != nil {
		log.Infof("Exists err: %v --> StatusInternalServerError (from %v)", err, req.RemoteAddr)
		http.Error(w, fmt.Sprintf("Error checking if bundle exists: %s", err), http.StatusInternalServerError)
		s.storeConfig.Stat.Counter(stats.BundlestoreUploadErrCounter).Inc(1)
		return
	}
	if ok {
		s.storeConfig.Stat.Counter(stats.BundlestoreUploadExistingCounter).Inc(1)
		fmt.Fprintf(w, "Bundle %s already exists, no-op and return\n", bundleName)
		return
	}

	// Get ttl if defaults were provided during Server construction or if it comes in this request header.
	var ttl *store.TTLValue
	if s.storeConfig.TTLCfg != nil {
		ttl = &store.TTLValue{TTL: time.Now().Add(s.storeConfig.TTLCfg.TTL), TTLKey: s.storeConfig.TTLCfg.TTLKey}
	}
	for k := range req.Header {
		if !strings.EqualFold(k, store.DefaultTTLKey) {
			continue
		}
		if ttlTime, err := time.Parse(time.RFC1123, req.Header.Get(k)); err != nil {
			log.Infof("TTL err: %v --> StatusInternalServerError (from %v)", err, req.RemoteAddr)
			http.Error(w, fmt.Sprintf("Error parsing TTL: %s", err), http.StatusInternalServerError)
			s.storeConfig.Stat.Counter(stats.BundlestoreUploadErrCounter).Inc(1)
			return
		} else if ttl != nil {
			ttl.TTL = ttlTime
		} else {
			ttl = &store.TTLValue{TTL: ttlTime, TTLKey: store.DefaultTTLKey}
		}
		break
	}
	if err := s.storeConfig.Store.Write(bundleName, store.NewResource(bundleData, req.ContentLength, ttl)); err != nil {
		log.Infof("Write err: %v --> StatusInternalServerError (from %v)", err, req.RemoteAddr)
		http.Error(w, fmt.Sprintf("Error writing Bundle: %s", err), http.StatusInternalServerError)
		s.storeConfig.Stat.Counter(stats.BundlestoreUploadErrCounter).Inc(1)
		return
	}
	fmt.Fprintf(w, "Successfully wrote bundle %s\n", bundleName)
	s.storeConfig.Stat.Counter(stats.BundlestoreUploadOkCounter).Inc(1)
}

func (s *httpServer) CheckExistence(w http.ResponseWriter, req *http.Request) {
	log.Infof("Checking Existence %v %v (from %v)", req.Host, req.URL, req.RemoteAddr)
	defer s.storeConfig.Stat.Latency(stats.BundlestoreCheckLatency_ms).Time().Stop()
	s.storeConfig.Stat.Counter(stats.BundlestoreCheckCounter).Inc(1)
	bundleName := strings.TrimPrefix(req.URL.Path, "/bundle/")
	if err := checkBundleName(bundleName); err != nil {
		log.Infof("Bundlename err: %v --> StatusBadRequest (from %v)", err, req.RemoteAddr)
		http.Error(w, err.Error(), http.StatusBadRequest)
		s.storeConfig.Stat.Counter(stats.BundlestoreCheckErrCounter).Inc(1)
		return
	}
	ok, err := s.storeConfig.Store.Exists(bundleName)
	if err != nil || !ok {
		log.Infof("Check err: %v --> StatusNotFound (from %v)", err, req.RemoteAddr)
		http.NotFound(w, req)
		s.storeConfig.Stat.Counter(stats.BundlestoreCheckErrCounter).Inc(1)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	s.storeConfig.Stat.Counter(stats.BundlestoreCheckOkCounter).Inc(1)
}

func (s *httpServer) HandleDownload(w http.ResponseWriter, req *http.Request) {
	log.Infof("Downloading %v %v (from %v)", req.Host, req.URL, req.RemoteAddr)
	defer s.storeConfig.Stat.Latency(stats.BundlestoreDownloadLatency_ms).Time().Stop()
	s.storeConfig.Stat.Counter(stats.BundlestoreDownloadCounter).Inc(1)
	bundleName := strings.TrimPrefix(req.URL.Path, "/bundle/")
	if err := checkBundleName(bundleName); err != nil {
		log.Infof("Bundlename err: %v --> StatusBadRequest (from %v)", err, req.RemoteAddr)
		http.Error(w, err.Error(), http.StatusBadRequest)
		//s.stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		s.storeConfig.Stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		return
	}

	r, err := s.storeConfig.Store.OpenForRead(bundleName)
	if err != nil {
		log.Infof("Read err: %v --> StatusNotFound (from %v)", err, req.RemoteAddr)
		http.NotFound(w, req)
		s.storeConfig.Stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		return
	}
	if _, err := io.Copy(w, r); err != nil {
		log.Infof("Copy err: %v --> StatusInternalServerError (from %v)", err, req.RemoteAddr)
		s.storeConfig.Stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		r.Close()
		return
	}
	if err := r.Close(); err != nil {
		log.Infof("Close err: %v --> StatusInternalServerError (from %v)", err, req.RemoteAddr)
		s.storeConfig.Stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		return
	}
	s.storeConfig.Stat.Counter(stats.BundlestoreDownloadOkCounter).Inc(1)
}

var bundleRE *regexp.Regexp = regexp.MustCompile("^bs-[a-z0-9]{40}.bundle")

// Check for name enforcement for HTTP API
func checkBundleName(name string) error {
	if ok := bundleRE.MatchString(name); ok {
		return nil
	}
	return fmt.Errorf("Error with bundleName, expected %q, got: %s", bundleRE, name)
}
