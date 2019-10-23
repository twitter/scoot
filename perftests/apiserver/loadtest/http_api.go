package loadtest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/endpoints"
)

/*
Process the /apiserver_test and /apiserver_test/kill service endpoints.

Example requests:
 - $ curl localhost:8080/apiserver_test?action=download&max_data_size=1000&min_data_size=10&num_times=20
 - $ curl localhost:8080/apiserver_test
 - $ curl localhost:8080/apiserver_test/kill
*/
func (lt *ApiserverLoadTester) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		if strings.Contains(r.RequestURI, "/kill") {
			lt.killEndpoint(w, r)
		} else {
			lt.getEndpoint(w, r)
		}
	default:
		resp := fmt.Sprintf("Method %s not implemented", r.Method)
		json.NewEncoder(w).Encode(resp)
	}
}

func (lt *ApiserverLoadTester) getEndpoint(w http.ResponseWriter, r *http.Request) {
	a := lt.getStringParam("action", "noAction", r)
	if a == "noAction" {
		lt.getTestResultEndpoint(w, r)
	} else {
		lt.startTest(w, r)
	}
}

func (lt *ApiserverLoadTester) getTestResultEndpoint(w http.ResponseWriter, r *http.Request) {
	status := lt.GetStatus()
	resp := status.String()
	json.NewEncoder(w).Encode(resp)
}

func (lt *ApiserverLoadTester) startTest(w http.ResponseWriter, r *http.Request) {
	// get the test parameters from the request
	lt.action = lt.getStringParam("action", "download", r)
	lt.minDataSetSize = lt.getIntParam("min_data_size", 1, r)
	lt.maxDataSetSize = lt.getIntParam("max_data_size", 1, r)
	lt.numActions = lt.getIntParam("num_times", 10, r)
	lt.freq = lt.getIntParam("freq", 0, r)
	lt.totalTime = lt.getIntParam("total_time", 30, r)

	go func() {
		lt.RunLoadTest()
	}()

	resp := "Starting load test."
	json.NewEncoder(w).Encode(resp)
}

func (lt *ApiserverLoadTester) killEndpoint(w http.ResponseWriter, r *http.Request) {
	lt.KillTest()
	resp := "Killing test.  Use GET to confirm that the test has stopped."
	json.NewEncoder(w).Encode(resp)
}

func (lt *ApiserverLoadTester) getStringParam(name string, deflt string, r *http.Request) string {
	t, ok := r.URL.Query()[name]
	if !ok {
		log.Infof("couldn't find param %s in query, defaulting to %s", name, deflt)
		return deflt
	}
	log.Infof("param %s in query, had value %s", name, t[0])
	return t[0]
}

func (lt *ApiserverLoadTester) getIntParam(name string, deflt int, r *http.Request) int {
	t := r.URL.Query()[name]
	if len(t) == 0 {
		log.Infof("couldn't find param %s in query, defaulting to %d", name, deflt)
		return deflt
	}
	n, err := strconv.Atoi(t[0])
	if err != nil {
		log.Errorf("%s, with value %s could not be converted to int, using default:%d",
			name, t, deflt)
		return deflt
	}
	log.Infof("param %s in query, had value %d", name, n)
	return n
}

/*
getEndpointHandlers exposes the /apiserver_test and /apiserver_test/kill endpoints.
*/
func (lt *ApiserverLoadTester) getEndpointHandlers() map[string]http.Handler {
	handlers := map[string]http.Handler{}
	handlers["/apiserver_test/kill"] = http.Handler(lt)
	handlers["/apiserver_test"] = http.Handler(lt)
	return handlers
}

// Start the HTTP service.
func (lt *ApiserverLoadTester) StartHttpServer(host string, port string) error {
	handlers := lt.getEndpointHandlers()
	addr := endpoints.Addr(fmt.Sprintf("%s:%s", host, port))
	server := endpoints.NewTwitterServer(addr, lt.getStatsReceiver(), handlers)
	return server.Serve()
}
