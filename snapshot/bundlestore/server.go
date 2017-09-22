package bundlestore

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/stats"
)

type Server struct {
	store  Store
	ttlCfg *TTLConfig
	stat   stats.StatsReceiver
}

// Make a new server that delegates to an underlying store.
// TTL may be nil, in which case defaults are applied downstream.
// TTL duration may be overriden by request headers, but we always pass this TTLKey to the store.
func MakeServer(s Store, ttl *TTLConfig, stat stats.StatsReceiver) *Server {
	scopedStat := stat.Scope("bundlestoreServer")
	go stats.StartUptimeReporting(scopedStat, stats.BundlestoreUptime_ms, stats.BundlestoreServerStartedGauge, stats.DefaultStartupGaugeSpikeLen)

	return &Server{s, ttl, scopedStat}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.stat.Counter(stats.BundlestoreRequestCounter).Inc(1) // TODO errata metric - remove if unused
	switch req.Method {
	case "POST":
		s.HandleUpload(w, req)
	case "HEAD":
		fallthrough
	case "GET":
		s.HandleDownload(w, req)
	default:
		// TODO(dbentley): do we need to support HEAD?
		log.Infof("Request err: %v --> StatusMethodNotAllowed (from %v)", req.Method, req.RemoteAddr)
		http.Error(w, "only support POST and GET", http.StatusMethodNotAllowed)
		return
	}
	s.stat.Counter(stats.BundlestoreRequestOkCounter).Inc(1) // TODO errata metric - remove if unused
}

func (s *Server) HandleUpload(w http.ResponseWriter, req *http.Request) {
	log.Infof("Uploading %v, %v, %v (from %v)", req.Host, req.URL, req.Header, req.RemoteAddr)
	defer s.stat.Latency(stats.BundlestoreUploadLatency_ms).Time().Stop()
	s.stat.Counter(stats.BundlestoreUploadCounter).Inc(1)
	bundleName := strings.TrimPrefix(req.URL.Path, "/bundle/")
	if err := s.checkBundleName(bundleName); err != nil {
		log.Infof("Bundlename err: %v --> StatusBadRequest (from %v)", err, req.RemoteAddr)
		http.Error(w, err.Error(), http.StatusBadRequest)
		s.stat.Counter(stats.BundlestoreUploadErrCounter).Inc(1)
		return
	}
	bundleData := req.Body

	exists, err := s.store.Exists(bundleName)
	if err != nil {
		log.Infof("Exists err: %v --> StatusInternalServerError (from %v)", err, req.RemoteAddr)
		http.Error(w, fmt.Sprintf("Error checking if bundle exists: %s", err), http.StatusInternalServerError)
		s.stat.Counter(stats.BundlestoreUploadErrCounter).Inc(1)
		return
	}
	if exists {
		s.stat.Counter(stats.BundlestoreUploadExistingCounter).Inc(1) // TODO errata metric - remove if unused
		fmt.Fprintf(w, "Bundle %s already exists, no-op and return\n", bundleName)
		return
	}

	// Get ttl if defaults were provided during Server construction or if it comes in this request header.
	var ttl *TTLValue
	if s.ttlCfg != nil {
		ttl = &TTLValue{time.Now().Add(s.ttlCfg.TTL), s.ttlCfg.TTLKey}
	}
	for k, _ := range req.Header {
		if !strings.EqualFold(k, DefaultTTLKey) {
			continue
		}
		if ttlTime, err := time.Parse(time.RFC1123, req.Header.Get(k)); err != nil {
			log.Infof("TTL err: %v --> StatusInternalServerError (from %v)", err, req.RemoteAddr)
			http.Error(w, fmt.Sprintf("Error parsing TTL: %s", err), http.StatusInternalServerError)
			s.stat.Counter(stats.BundlestoreUploadErrCounter).Inc(1)
			return
		} else if ttl != nil {
			ttl.TTL = ttlTime
		} else {
			ttl = &TTLValue{ttlTime, DefaultTTLKey}
		}
		break
	}
	if err := s.store.Write(bundleName, bundleData, ttl); err != nil {
		log.Infof("Write err: %v --> StatusInternalServerError (from %v)", err, req.RemoteAddr)
		http.Error(w, fmt.Sprintf("Error writing Bundle: %s", err), http.StatusInternalServerError)
		s.stat.Counter(stats.BundlestoreUploadErrCounter).Inc(1)
		return
	}
	fmt.Fprintf(w, "Successfully wrote bundle %s\n", bundleName)
	s.stat.Counter(stats.BundlestoreUploadOkCounter).Inc(1)
}

func (s *Server) HandleDownload(w http.ResponseWriter, req *http.Request) {
	log.Infof("Downloading %v %v (from %v)", req.Host, req.URL, req.RemoteAddr)
	defer s.stat.Latency(stats.BundlestoreDownloadLatency_ms).Time().Stop()
	s.stat.Counter(stats.BundlestoreDownloadCounter).Inc(1)
	bundleName := strings.TrimPrefix(req.URL.Path, "/bundle/")
	if err := s.checkBundleName(bundleName); err != nil {
		log.Infof("Bundlename err: %v --> StatusBadRequest (from %v)", err, req.RemoteAddr)
		http.Error(w, err.Error(), http.StatusBadRequest)
		s.stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		return
	}

	r, err := s.store.OpenForRead(bundleName)
	if err != nil {
		log.Infof("Read err: %v --> StatusNotFound (from %v)", err, req.RemoteAddr)
		http.NotFound(w, req)
		s.stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		return
	}
	if _, err := io.Copy(w, r); err != nil {
		log.Infof("Copy err: %v --> StatusInternalServerError (from %v)", err, req.RemoteAddr)
		http.Error(w, fmt.Sprintf("Error copying Bundle: %s", err), http.StatusInternalServerError)
		s.stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		return
	}
	if err := r.Close(); err != nil {
		log.Infof("Close err: %v --> StatusInternalServerError (from %v)", err, req.RemoteAddr)
		http.Error(w, fmt.Sprintf("Error closing Bundle Data: %s", err), http.StatusInternalServerError)
		s.stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		return
	}
	s.stat.Counter(stats.BundlestoreDownloadOkCounter).Inc(1)
}

// TODO(dbentley): comprehensive check if it's a legal bundle name. See README.md.
func (s *Server) checkBundleName(name string) error {
	bundleRE := "^bs-[a-z0-9]{40}.bundle"
	if ok, _ := regexp.MatchString(bundleRE, name); ok {
		return nil
	}
	return fmt.Errorf("Error with bundleName, expected %q, got: %s", bundleRE, name)
}
