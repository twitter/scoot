package loadtest
/*
start the load testers as an http server exposing the endpoints:
get apiserver_test - returns the status (job running, waiting to start, etc.)
get apiserver_test?action=<upload/download>&... - starts a load test
get apiserver_test/kill - kills the current running test
 */

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

func (lt *ApiserverLoadTester) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		if strings.Contains(r.RequestURI, "/kill") {
			lt.KillEndpoint(w, r)
		} else {
			lt.GetEndpoint(w, r)
		}
	default:
		resp := fmt.Sprintf("Method %s not implemented", r.Method)
		json.NewEncoder(w).Encode(resp)
	}
}

func (lt *ApiserverLoadTester) GetEndpoint(w http.ResponseWriter, r *http.Request) {
	a := lt.getStringParam("action", "noAction", r)
	if a == "noAction" {
		lt.GetTestResultEndpoint(w, r)
	} else {
		lt.StartTestEndpoint(w, r)
	}
}

func (lt *ApiserverLoadTester) GetTestResultEndpoint(w http.ResponseWriter, r *http.Request) {
	status := lt.GetStatus()
	resp := status.String()
	json.NewEncoder(w).Encode(resp)
}

func (lt *ApiserverLoadTester) StartTestEndpoint(w http.ResponseWriter, r *http.Request) {
	// get the test parameters from the request
	lt.action = lt.getStringParam("action", "download", r)
	lt.minDataSetSize = lt.getIntParam("min_data_size", 1, r)
	lt.maxDataSetSize = lt.getIntParam("max_data_size", 1, r)
	lt.numConcurrentActs = lt.getIntParam("num_times", 10, r)
	lt.freq = lt.getIntParam("freq", 0, r)
	lt.totalTime = lt.getIntParam("total_time", 30, r)

	go func() {
		lt.RunLoadTest()
	} ()

	resp := "Starting load test."
	json.NewEncoder(w).Encode(resp)
}


func (lt *ApiserverLoadTester) KillEndpoint(w http.ResponseWriter, r *http.Request) {
	lt.KillTest()
	resp := "Killing test.  Use GET to confirm that the test has stopped."
	json.NewEncoder(w).Encode(resp)
}


func (lt *ApiserverLoadTester) getStringParam(name string, deflt string, r *http.Request) string {
	t, ok := r.URL.Query()[name]
	if !ok {
		return deflt
	}
	return t[0]
}

func (lt *ApiserverLoadTester) getIntParam(name string, deflt int, r *http.Request) int {
	t := r.URL.Query()[name]
	if len(t) == 0 {
		return deflt
	}
	n, err := strconv.Atoi(t[0])
	if err != nil {
		log.Errorf("%s, with value %s could not be converted to int, using default:%d",
			name, t, deflt)
		return deflt
	}
	return n
}

func (lt *ApiserverLoadTester) GetEndpointHandlers() map[string]http.Handler {
	handlers := map[string]http.Handler{}
	handlers["/apiserver_test/kill"] = http.Handler(lt)
	handlers["/apiserver_test"] = http.Handler(lt)
	return handlers
}
