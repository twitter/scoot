package snapshots

import (
	"net/http"
	"strings"

	"github.com/twitter/scoot/snapshot"
)

// ViewServer allows viewing files in a Snapshot
type ViewServer struct {
	db snapshot.DB
}

// NewViewServer creates a new ViewServer to serve snapshots in DB
func NewViewServer(db snapshot.DB) *ViewServer {
	return &ViewServer{db}
}

// Serve requests to View files in a Snapshot
func (s *ViewServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	idAndPath := strings.TrimPrefix(req.URL.Path, "/view/")
	parts := strings.SplitN(idAndPath, "/", 2)
	if len(parts) != 2 {
		http.Error(w, "need both an id and a path", http.StatusBadRequest)
		return
	}

	id, path := parts[0], parts[1]
	data, err := s.db.ReadFileAll(snapshot.ID(id), path)
	if err != nil {
		// TODO(dbentley): we should figure out what the error is
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Write(data)
}
