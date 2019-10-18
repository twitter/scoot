/*
The loadtest package implements running performance benchmarking against the apiserver.  It can be used to benchmark upload and download performance.
Benchmarking can be run as:
	- a one-time test issuing user directed upload/download volume to the apiserver
	- a continuous test repeatedly issuing user direct upload/download volume to the apiserver
	- as an http service accepting http requests to place one-time or continuous upload/download volume on the server

Performance metrics (p50, p95 of read/write latencies, etc.) are appended to a temp file.  (The location of the temp file
is printed when the test is run.)

The methods in http_api.go implement the http endpoints:

(GET) /apiserver_test - returns the status (job running, waiting to start, etc.)

(GET) /apiserver_test?action=<upload/download>&... - starts a load test  Note: using POST to start a test would be
better aligned with REST philosophy, but then we would have to pass the test arguments in on the body of the
request when we are using curl, making the curl experience more klunky.  Thus we are using GET to trigger a test.

(GET) apiserver_test/kill - kills the current running test
*/
package loadtest
