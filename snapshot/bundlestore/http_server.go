package bundlestore

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/stats"
)

type httpServer struct {
	stuff *CommonStuff
	//store  Store
	//ttlCfg *TTLConfig
	//stat   stats.StatsReceiver
}

//func MakeHTTPServer(s Store, ttl *TTLConfig, stat stats.StatsReceiver) *httpServer {
//	return &httpServer{s, ttl, stat}
//}
func MakeHTTPServer(stuffz *CommonStuff) *httpServer {
	return &httpServer{stuffz}
}

func (s *httpServer) HandleUpload(w http.ResponseWriter, req *http.Request) {
	log.Infof("Uploading %v, %v, %v (from %v)", req.Host, req.URL, req.Header, req.RemoteAddr)
	//defer s.stat.Latency(stats.BundlestoreUploadLatency_ms).Time().Stop()
	defer s.stuff.stat.Latency(stats.BundlestoreUploadLatency_ms).Time().Stop()
	//s.stat.Counter(stats.BundlestoreUploadCounter).Inc(1)
	s.stuff.stat.Counter(stats.BundlestoreUploadCounter).Inc(1)
	bundleName := strings.TrimPrefix(req.URL.Path, "/bundle/")
	if err := CheckBundleName(bundleName); err != nil {
		log.Infof("Bundlename err: %v --> StatusBadRequest (from %v)", err, req.RemoteAddr)
		http.Error(w, err.Error(), http.StatusBadRequest)
		//s.stat.Counter(stats.BundlestoreUploadErrCounter).Inc(1)
		s.stuff.stat.Counter(stats.BundlestoreUploadErrCounter).Inc(1)
		return
	}
	bundleData := req.Body

	//exists, err := s.store.Exists(bundleName)
	exists, err := s.stuff.store.Exists(bundleName)
	if err != nil {
		log.Infof("Exists err: %v --> StatusInternalServerError (from %v)", err, req.RemoteAddr)
		http.Error(w, fmt.Sprintf("Error checking if bundle exists: %s", err), http.StatusInternalServerError)
		//s.stat.Counter(stats.BundlestoreUploadErrCounter).Inc(1)
		s.stuff.stat.Counter(stats.BundlestoreUploadErrCounter).Inc(1)
		return
	}
	if exists {
		//s.stat.Counter(stats.BundlestoreUploadExistingCounter).Inc(1)
		s.stuff.stat.Counter(stats.BundlestoreUploadExistingCounter).Inc(1)
		fmt.Fprintf(w, "Bundle %s already exists, no-op and return\n", bundleName)
		return
	}

	// Get ttl if defaults were provided during Server construction or if it comes in this request header.
	var ttl *TTLValue
	//if s.ttlCfg != nil {
	if s.stuff.ttlCfg != nil {
		//ttl = &TTLValue{time.Now().Add(s.ttlCfg.TTL), s.ttlCfg.TTLKey}
		ttl = &TTLValue{time.Now().Add(s.stuff.ttlCfg.TTL), s.stuff.ttlCfg.TTLKey}
	}
	for k, _ := range req.Header {
		if !strings.EqualFold(k, DefaultTTLKey) {
			continue
		}
		if ttlTime, err := time.Parse(time.RFC1123, req.Header.Get(k)); err != nil {
			log.Infof("TTL err: %v --> StatusInternalServerError (from %v)", err, req.RemoteAddr)
			http.Error(w, fmt.Sprintf("Error parsing TTL: %s", err), http.StatusInternalServerError)
			//s.stat.Counter(stats.BundlestoreUploadErrCounter).Inc(1)
			s.stuff.stat.Counter(stats.BundlestoreUploadErrCounter).Inc(1)
			return
		} else if ttl != nil {
			ttl.TTL = ttlTime
		} else {
			ttl = &TTLValue{ttlTime, DefaultTTLKey}
		}
		break
	}
	//if err := s.store.Write(bundleName, bundleData, ttl); err != nil {
	if err := s.stuff.store.Write(bundleName, bundleData, ttl); err != nil {
		log.Infof("Write err: %v --> StatusInternalServerError (from %v)", err, req.RemoteAddr)
		http.Error(w, fmt.Sprintf("Error writing Bundle: %s", err), http.StatusInternalServerError)
		//s.stat.Counter(stats.BundlestoreUploadErrCounter).Inc(1)
		s.stuff.stat.Counter(stats.BundlestoreUploadErrCounter).Inc(1)
		return
	}
	fmt.Fprintf(w, "Successfully wrote bundle %s\n", bundleName)
	//s.stat.Counter(stats.BundlestoreUploadOkCounter).Inc(1)
	s.stuff.stat.Counter(stats.BundlestoreUploadOkCounter).Inc(1)
}

func (s *httpServer) HandleDownload(w http.ResponseWriter, req *http.Request) {
	log.Infof("Downloading %v %v (from %v)", req.Host, req.URL, req.RemoteAddr)
	//defer s.stat.Latency(stats.BundlestoreDownloadLatency_ms).Time().Stop()
	defer s.stuff.stat.Latency(stats.BundlestoreDownloadLatency_ms).Time().Stop()
	//s.stat.Counter(stats.BundlestoreDownloadCounter).Inc(1)
	s.stuff.stat.Counter(stats.BundlestoreDownloadCounter).Inc(1)
	bundleName := strings.TrimPrefix(req.URL.Path, "/bundle/")
	if err := CheckBundleName(bundleName); err != nil {
		log.Infof("Bundlename err: %v --> StatusBadRequest (from %v)", err, req.RemoteAddr)
		http.Error(w, err.Error(), http.StatusBadRequest)
		//s.stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		s.stuff.stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		return
	}

	//r, err := s.store.OpenForRead(bundleName)
	r, err := s.stuff.store.OpenForRead(bundleName)
	if err != nil {
		log.Infof("Read err: %v --> StatusNotFound (from %v)", err, req.RemoteAddr)
		http.NotFound(w, req)
		//s.stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		s.stuff.stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		return
	}
	if _, err := io.Copy(w, r); err != nil {
		log.Infof("Copy err: %v --> StatusInternalServerError (from %v)", err, req.RemoteAddr)
		http.Error(w, fmt.Sprintf("Error copying Bundle: %s", err), http.StatusInternalServerError)
		//s.stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		s.stuff.stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		return
	}
	if err := r.Close(); err != nil {
		log.Infof("Close err: %v --> StatusInternalServerError (from %v)", err, req.RemoteAddr)
		http.Error(w, fmt.Sprintf("Error closing Bundle Data: %s", err), http.StatusInternalServerError)
		//s.stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		s.stuff.stat.Counter(stats.BundlestoreDownloadErrCounter).Inc(1)
		return
	}
	//s.stat.Counter(stats.BundlestoreDownloadOkCounter).Inc(1)
	s.stuff.stat.Counter(stats.BundlestoreDownloadOkCounter).Inc(1)
}
