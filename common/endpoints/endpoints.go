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
		Addr:            addr,
		Stats:           stats,
		ResourceHandler: &ResourceHandler{resources: make(map[string]map[string]string)},
	}
}

type TwitterServer struct {
	Addr            string
	Stats           stats.StatsReceiver
	ResourceHandler *ResourceHandler
}

func (s *TwitterServer) Serve() error {
	http.HandleFunc("/", helpHandler)
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/admin/metrics.json", s.statsHandler)
	log.Println("Serving http & stats on", s.Addr)
	return http.ListenAndServe(s.Addr, nil)
}

func helpHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Common paths: '/health', '/admin/metrics.json', '/{NAMESPACE}/stdout', '/{NAMESPACE}/stderr'", 501)
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

const StdoutName = "stdout"
const StderrName = "stderr"

type ResourceHandler struct {
	//defines: map[Namespace]map[ResourceName]ResourcePath
	resources map[string]map[string]string
}

func (h *ResourceHandler) AddResource(namespace, name, path string) {
	if _, ok := h.resources[namespace]; !ok {
		h.resources[namespace] = make(map[string]string)
	}
	h.resources[namespace][name] = path
	http.Handle(fmt.Sprintf("/%s/%s", namespace, name), h)
}

func (h *ResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//TODO:
	//Add a second option beyond text - serve barebones vanilla javascript that does ajax updates to tail resource.
	// id := ""
	// pos := 0
	// name := ""
	// path := h.idToNamedPaths[id][name]
	// io.Copy(w, path[pos], len)
}
