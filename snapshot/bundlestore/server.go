package bundlestore

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
)

type Server struct {
	store Store
	addr  string
}

type Addr string

func MakeServer(s Store, a Addr) *Server {
	return &Server{s, string(a)}
}

func (s *Server) Serve() error {
	mux := http.NewServeMux()
	mux.Handle("/bundle/", s)
	server := &http.Server{
		Addr:    s.addr,
		Handler: mux,
	}
	log.Println("Serving Bundles on", s.addr)
	return server.ListenAndServe()
}

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
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
	}
}

func (s *Server) HandleUpload(w http.ResponseWriter, req *http.Request) {
	log.Printf("Uploading %s", req.URL.Path)
	bundleName := strings.TrimPrefix(req.URL.Path, "/bundle/")
	if ok, err := s.checkBundleName(bundleName); !ok {
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
		fmt.Fprintf(w, "Bundle %s already exists\n", bundleName)
	}

	if err := s.store.Write(bundleName, bundleData); err != nil {
		http.Error(w, fmt.Sprintf("Error writing Bundle: %s", err), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "Successfully wrote bundle %s\n", bundleName)
}

func (s *Server) HandleDownload(w http.ResponseWriter, req *http.Request) {
	bundleName := strings.TrimPrefix(req.URL.Path, "/bundle/")
	log.Printf("Downloading %s", bundleName)
	if ok, err := s.checkBundleName(bundleName); !ok {
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
}

// TODO(dbentley): comprehensive check if it's a legal bundle name, for now just '%s-%s-%s'
func (s *Server) checkBundleName(name string) (bool, error) {
	if ok, _ := regexp.MatchString("^[^-/]+-[^-/]+-[^-/]+.*", name); ok {
		return true, nil
	} else {
		return false, fmt.Errorf("Error with bundleName, expected '%%s-%%s-%%s', got: %s", name)
	}
}
