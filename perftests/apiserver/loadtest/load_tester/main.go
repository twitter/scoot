/*
load_tester is the cli  for generating load on the apiserver.

The load scenario is parameterized in terms of action (upload/download/both),
file size (min, max), number of (concurrent) actions, frequency for running a scenario (once, every N minutes)

The command line options allow the user to request that the test be run to:
	- place a one time upload/download load on the api server,
	- repeatedly place an upload/download load on the apiserver
	- can be run as an http service waiting get requests to trigger the test.  See perftests/apiserver/loadtest/http_api

sample run:
load_tester -action=both -cas_addr=<apiserver> -log_level=info -max_data_size=1000 -min_data_size=10 -num_times=30
*/
package main

import (
	"flag"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/perftests/apiserver/loadtest"
)

/*
This is the main for the apiserver load tester
*/
func main() {
	log.AddHook(hooks.NewContextHook())

	// parse the arguments
	a, startAsServer, server, port, err := argParse()
	if err != nil {
		log.Fatal(err)
		return
	}

	level, err := log.ParseLevel(a.LogLevel)
	if err != nil {
		log.Fatal(err)
		return
	}
	log.SetLevel(level)

	// make the load tester
	lt := loadtest.MakeApiserverLoadTester(a)

	// start the server or run the one-time test
	if startAsServer { // start as http service, waiting for run requests
		err = lt.StartHttpServer(server, port)
		if err != nil {
			log.Fatal(err)
		}
	} else { // run the test directly from cli
		e := lt.RunLoadTest()
		if e != nil {
			log.Errorf("%s", e.Error())
		}
	}

	log.Info("load test completed.")
}

func argParse() (*loadtest.Args, bool, string, string, error) {
	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
	actionFlag := flag.String("action", "download", "upload/download/both: request testing upload, download or an equal mix of both.")
	dataSizeMinFlag := flag.Int("min_data_size", 1,
		fmt.Sprintf("minimum data set size (in K bytes) for testing. Must be one of %s", loadtest.TestDataSizesStr))
	dataSizeMaxFlag := flag.Int("max_data_size", 1,
		fmt.Sprintf("maximum data size (in K bytes) for testing. Must be one of %s", loadtest.TestDataSizesStr))
	numTimes := flag.Int("num_times", 10, "number of times the action should be taken (concurrently)")
	batch := flag.Bool("batch", false, "Use the batch api consolidating all upload or download actions in one CAS call. (default false)")
	frequencyFlag := flag.Int("freq", 0,
		"0 requests to run the test once, > 0 requests to run 100 iterations of the test every <freq> minutes")
	totalTime := flag.Int("total_time", 30, "Total number of minutes to allow the test to run (default 30)")
	casGrpcAddr := flag.String("cas_addr", "", "cas grpc address")
	startAsServerFlag := flag.Bool("start_as_http_server", false, "start an http service listening for get to trigger the test. Default false.")
	serverNameFlag := flag.String("server_name", "localhost", "name for this load test server. Default localhost")
	portFlag := flag.String("port", "", "The (load test) server's portFlag.")
	flag.Parse()

	a := &loadtest.Args{
		LogLevel:    strings.ToLower(*logLevelFlag),
		Action:      strings.ToLower(*actionFlag),
		DataSizeMin: *dataSizeMinFlag,
		DataSizeMax: *dataSizeMaxFlag,
		NumTimes:    *numTimes,
		Freq:        *frequencyFlag,
		TotalTime:   *totalTime,
		CasGrpcAddr: *casGrpcAddr,
		Batch:       *batch,
	}
	if !validateArgs(a, *startAsServerFlag, *portFlag) {
		return a, false, "", "", fmt.Errorf("bad arugment value(s)")
	}

	return a, *startAsServerFlag, *serverNameFlag, *portFlag, nil
}

/*
validate the command line arguments values
*/
func validateArgs(a *loadtest.Args, startAsServer bool, port string) bool {
	err := false
	if a.DataSizeMax < a.DataSizeMin {
		log.Fatalf("max_file_size must be > min_file_size.")
		err = true
	}
	if ok := validateFileSize(a.DataSizeMin); !ok {
		log.Fatalf("min_file_size must be one of %s", loadtest.TestDataSizesStr)
		err = true
	}

	if ok := validateFileSize(a.DataSizeMax); !ok {
		log.Fatalf("max_file_size must be one of %s", loadtest.TestDataSizesStr)
		err = true
	}
	if !(a.Action == "upload" || a.Action == "download" ||
		a.Action == "both") {
		log.Fatalf("action must be one of upload, download or both.")
		err = true
	}
	if a.Action == "both" && a.Batch {
		log.Fatalf("action must be upload or download on batch runs.")
		err = true
	}
	if a.NumTimes < 1 {
		log.Fatalf("num_times must be > 0.")
		err = true
	}
	if a.Freq < 0 {
		log.Fatalf("frequency must be >= 0.")
		err = true
	}
	if a.TotalTime <= 0 {
		log.Fatalf("totalTime must be > 0.")
		err = true
	}
	if a.Batch && a.Action == "both" {
		log.Fatalf("batch cannot be used with 'both' action pick one of upload/download.")
		err = true
	}

	if startAsServer {
		if a.CasGrpcAddr == "" {
			log.Fatalf("casGrpcAddr must be defined.")
			err = true
		}
		if port == "" {
			log.Fatalf("port must be defined.")
			err = true
		}
	}
	return !err
}

func validateFileSize(size int) bool {
	ok := false
	for i := 0; i < len(loadtest.TestDataSizes); i++ {
		if size == loadtest.TestDataSizes[i] {
			ok = true
			break
		}
	}
	return ok
}
