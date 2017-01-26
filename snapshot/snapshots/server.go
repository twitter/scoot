package snapshots

import (
	"net/http"
	"strings"

	"github.com/scootdev/scoot/snapshot"
)

type ViewServer struct {
	db snapshot.DB
}

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
