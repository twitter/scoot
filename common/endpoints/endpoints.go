package endpoints

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/scootdev/scoot/common/stats"
)

func RegisterHealthCheck(path string) {
	http.HandleFunc(path, healthHandler)
}

func RegisterStats(path string, statsRecvr stats.StatsReceiver) {
	stats.CurrentStatsReceiver = statsRecvr
	http.HandleFunc(path, statsHandler)
}

func Serve(addr string) {
	log.Println("Serving http & stats on", addr)
	err := http.ListenAndServe(addr, nil)
	log.Println("Done serving http", err)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "ok")
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	const contentTypeHdr = "Content-Type"
	const contentTypeVal = "application/json; charset=utf-8"
	w.Header().Set(contentTypeHdr, contentTypeVal)

	pretty := r.URL.Query().Get("pretty") == "true"
	str := stats.CurrentStatsReceiver.Render(pretty)
	if _, err := io.Copy(w, bytes.NewBuffer(str)); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
}
