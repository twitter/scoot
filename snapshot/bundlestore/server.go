package bundlestore

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/scootdev/scoot/common/stats"
)

type Server struct {
	store Store
	stat  stats.StatsReceiver
}

func MakeServer(s Store, stat stats.StatsReceiver) *Server {
	return &Server{s, stat.Scope("bundlestoreServer")}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.stat.Counter("serveCounter").Inc(1)
	switch req.Method {
	case "POST":
		s.HandleUpload(w, req)
	case "HEAD":
		fallthrough
	case "GET":
		s.HandleDownload(w, req)
	default:
		// TODO(dbentley): do we need to support HEAD?
		http.Error(w, "only support POST and GET", http.StatusMethodNotAllowed)
		return
	}
	s.stat.Counter("serveOkCounter").Inc(1)
}

func (s *Server) HandleUpload(w http.ResponseWriter, req *http.Request) {
	log.Infof("Uploading %v, %v", req.Host, req.URL)
	defer s.stat.Latency("uploadLatency_ms").Time().Stop()
	s.stat.Counter("uploadCounter").Inc(1)
	bundleName := strings.TrimPrefix(req.URL.Path, "/bundle/")
	if err := s.checkBundleName(bundleName); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	bundleData := req.Body

	exists, err := s.store.Exists(bundleName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error checking if bundle exists: %s", err), http.StatusInternalServerError)
		return
	}
	if exists {
		s.stat.Counter("uploadExistingCounter").Inc(1)
		fmt.Fprintf(w, "Bundle %s already exists\n", bundleName)
	}

	if err := s.store.Write(bundleName, bundleData); err != nil {
		http.Error(w, fmt.Sprintf("Error writing Bundle: %s", err), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "Successfully wrote bundle %s\n", bundleName)
	s.stat.Counter("uploadOkCounter").Inc(1)
}

func (s *Server) HandleDownload(w http.ResponseWriter, req *http.Request) {
	log.Infof("Downloading %v %v", req.Host, req.URL)
	defer s.stat.Latency("downloadLatency_ms").Time().Stop()
	s.stat.Counter("downloadCounter").Inc(1)
	bundleName := strings.TrimPrefix(req.URL.Path, "/bundle/")
	if err := s.checkBundleName(bundleName); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	r, err := s.store.OpenForRead(bundleName)
	if err != nil {
		http.NotFound(w, req)
		return
	}
	if _, err := io.Copy(w, r); err != nil {
		http.Error(w, fmt.Sprintf("Error writing Bundle: %s", err), http.StatusInternalServerError)
		return
	}
	if err := r.Close(); err != nil {
		http.Error(w, fmt.Sprintf("Error closing Bundle Data: %s", err), http.StatusInternalServerError)
		return
	}
	s.stat.Counter("downloadOkCounter").Inc(1)
}

// TODO(dbentley): comprehensive check if it's a legal bundle name. See README.md.
func (s *Server) checkBundleName(name string) error {
	bundleRE := "^bs-[a-z0-9]{40}.bundle"
	if ok, _ := regexp.MatchString(bundleRE, name); ok {
		return nil
	}
	return fmt.Errorf("Error with bundleName, expected %q, got: %s", bundleRE, name)
}
