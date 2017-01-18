// Wrappers for receivers from the common/stats package and setting
// up an HTTP server with endpoints to make the stats data accessible.
package endpoints

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/scootdev/scoot/common/stats"
)

// Returns an http handler at 'addr' which can retrieved with 'uri' if specified. Also serves static files from tmpDir if specified.
func NewTwitterServer(addr string, stats stats.StatsReceiver, handlers map[string]http.Handler) *TwitterServer {
	return NewTwitterServerWithSidecar(addr, stats, handlers, nil)
}

func NewTwitterServerWithSidecar(
	addr string,
	stats stats.StatsReceiver,
	handlers map[string]http.Handler,
	sidecarStatPorts []int) *TwitterServer {
	return &TwitterServer{
		Addr:             addr,
		Stats:            stats,
		Handlers:         handlers,
		SidecarStatPorts: sidecarStatPorts,
	}
}

// A stats receiver that provides HTTP access for metric scraping with
// Twitter-style endpoints.
type TwitterServer struct {
	Addr             string
	Stats            stats.StatsReceiver
	Handlers         map[string]http.Handler
	SidecarStatPorts []int
}

func (s *TwitterServer) Serve() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", helpHandler)
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/admin/metrics.json", s.statsHandler)
	for path, handler := range s.Handlers {
		mux.Handle(path, handler)
	}
	log.Println("Serving http & stats on", s.Addr)
	server := &http.Server{
		Addr:    s.Addr,
		Handler: mux,
	}
	return server.ListenAndServe()
}

func helpHandler(w http.ResponseWriter, r *http.Request) {
	msg := "Common paths: '/health', '/admin/metrics.json', '/output'"
	http.Error(w, msg, http.StatusNotImplemented)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "ok")
}

func (s *TwitterServer) statsHandler(w http.ResponseWriter, r *http.Request) {
	const contentTypeHdr = "Content-Type"
	const contentTypeVal = "application/json; charset=utf-8"
	w.Header().Set(contentTypeHdr, contentTypeVal)

	// get stats from this Twitter Server server
	serviceStats := s.Stats.Render(false)

	var allStats map[string]float64
	json.Unmarshal(serviceStats, &allStats)

	// get stats for the sidecars
	sidecarStatsCh := make(chan []byte, len(s.SidecarStatPorts))
	var wg sync.WaitGroup
	for _, p := range s.SidecarStatPorts {
		wg.Add(1)
		go func(port int) {
			defer wg.Done()
			// sidecars must be running on the same machine and also be twitter servers
			client := http.Client{
				Timeout: 1 * time.Second,
			}
			rsp, err := client.Get(fmt.Sprintf("http://localhost:%v/admin/metrics.json", port))
			if err == nil {
				defer rsp.Body.Close()
				body, err := ioutil.ReadAll(rsp.Body)

				if err == nil {
					sidecarStatsCh <- body

				} else {
					log.Printf("ERROR: Reading Body of Stats Response for Sidecar at Port %v, Error: %v", port, err)
				}
			} else {
				log.Printf("ERROR: Getting Stats for Sidecar at Port %v, returned an error %v", port, err)
			}
		}(p)
	}

	// wait until we have all the stats then combine
	wg.Wait()
	close(sidecarStatsCh)
	for statsRsp := range sidecarStatsCh {
		// just reuse same map, since new keys get added
		json.Unmarshal(statsRsp, &allStats)
	}

	// format stats and return
	var statsAsBinary []byte
	var err error
	pretty := r.URL.Query().Get("pretty") == "true"

	if pretty {
		statsAsBinary, err = json.MarshalIndent(allStats, "", "  ")
	} else {
		statsAsBinary, err = json.Marshal(allStats)
	}

	if _, err = io.Copy(w, bytes.NewBuffer(statsAsBinary)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
