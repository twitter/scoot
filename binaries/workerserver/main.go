package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/workerapi/server"
)

var thriftPort = flag.Int("thrift_port", 9090, "port to serve thrift on")
var httpPort = flag.Int("http_port", 9091, "port to serve http on")

func main() {
	flag.Parse()
	go serveHTTP()

	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transportFactory := thrift.NewTTransportFactory()

	err := server.Serve(server.NewHandler(), fmt.Sprintf(":%d", *thriftPort), transportFactory, protocolFactory)
	if err != nil {
		log.Fatal("Error serving Worker Server: ", err)
	}
}

func handler(w http.ResponseWriter, r *http.Request) {
	log.Println("Handling!", r.URL.Path)
	// TODO(dbentley): implement the actual health-checking protocol.
	fmt.Fprintf(w, "OK (health check in golang for %v )", r.URL.Path)
}

func serveHTTP() {
	log.Println("Serving http on", *httpPort)
	http.HandleFunc("/", handler)
	err := http.ListenAndServe(fmt.Sprintf(":%d", *httpPort), nil)
	log.Println("Done serving http", err)
}
