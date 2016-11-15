// Wrappers for receivers from the common/stats package and setting
// up an HTTP server with endpoints to make the stats data accessible.
package endpoints

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/scootdev/scoot/common/stats"
)

func NewTwitterServer(addr string, stats stats.StatsReceiver) *TwitterServer {
	return &TwitterServer{
		Addr:  addr,
		Stats: stats,
	}
}

type TwitterServer struct {
	Addr  string
	Stats stats.StatsReceiver
}

func (s *TwitterServer) Serve() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", healthHandler)
	mux.HandleFunc("/admin/metrics.json", s.statsHandler)
	server := &http.Server{
		Addr:    s.Addr,
		Handler: mux,
	}
	log.Println("Serving http & stats on", s.Addr)
	return server.ListenAndServe()
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "ok")
}

func (s *TwitterServer) statsHandler(w http.ResponseWriter, r *http.Request) {
	const contentTypeHdr = "Content-Type"
	const contentTypeVal = "application/json; charset=utf-8"
	w.Header().Set(contentTypeHdr, contentTypeVal)

	pretty := r.URL.Query().Get("pretty") == "true"
	str := s.Stats.Render(pretty)
	if _, err := io.Copy(w, bytes.NewBuffer(str)); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}

type StatScope string

func MakeStatsReceiver(scope StatScope) stats.StatsReceiver {
	s, _ := stats.NewCustomStatsReceiver(
		stats.NewFinagleStatsRegistry,
		15*time.Second)
	return s.Scope(string(scope))
}
