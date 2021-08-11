// Wrappers for receivers from the common/stats package and setting
// up an HTTP server with endpoints to make the stats data accessible.
package endpoints

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/stats"
)

type Addr string

// Returns an http handler at 'addr'
func NewTwitterServer(addr Addr, stats stats.StatsReceiver, handlers map[string]http.Handler) *TwitterServer {
	return &TwitterServer{
		Addr:     string(addr),
		Stats:    stats,
		Handlers: handlers,
	}
}

// A stats receiver that provides HTTP access for metric scraping with
// Twitter-style endpoints.
type TwitterServer struct {
	Addr     string
	Stats    stats.StatsReceiver
	Handlers map[string]http.Handler
}

func (s *TwitterServer) String() string {
	return fmt.Sprintf("TwitterServer:Addr:%s, Stats:%+v, Handlers:%+v", s.Addr, s.Stats, s.Handlers)
}

func (s *TwitterServer) Serve() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", helpHandler)
	mux.HandleFunc("/admin/metrics.json", s.statsHandler)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	for path, handler := range s.Handlers {
		mux.Handle(path, handler)
	}
	log.Info("Serving http & stats on: ", s.Addr)
	server := &http.Server{
		Addr:    s.Addr,
		Handler: mux,
	}
	return server.ListenAndServe()
}

func helpHandler(w http.ResponseWriter, r *http.Request) {
	msg := "Common paths: '/admin/metrics.json', '/debug/pprof'"
	http.Error(w, msg, http.StatusNotImplemented)
}

func (s *TwitterServer) statsHandler(w http.ResponseWriter, r *http.Request) {
	const contentTypeHdr = "Content-Type"
	const contentTypeVal = "application/json; charset=utf-8"
	w.Header().Set(contentTypeHdr, contentTypeVal)

	pretty := r.URL.Query().Get("pretty") == "true"
	str := s.Stats.Render(pretty)
	if _, err := io.Copy(w, bytes.NewBuffer(str)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type StatScope string

// Create a finagle-style stats receiver with a reasonable latch default, minutely.
func MakeStatsReceiver(scope StatScope) stats.StatsReceiver {
	s, _ := stats.NewCustomStatsReceiver(
		stats.NewFinagleStatsRegistry,
		60*time.Second)
	return s.Scope(string(scope))
}
