package main

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/scootdev/scoot/scootapi/client"
	"log"
)

// Command line tool to talk to CloudScoot Server
// Check Job Status:
// 			get_job_status "job1"
// Run Smoke Test:
//      run_smoke_test

// Binary to talk to Cloud Scoot API
func main() {
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
	transportFactory := thrift.NewTTransportFactory()

	client, err := client.NewClient(transportFactory, protocolFactory)
	if err != nil {
		log.Fatal("Cannot initialize Cloud Scoot CLI: ", err)
	}
	err = client.Exec()
	if err != nil {
		log.Fatal("error running scootapi ", err)
	}
}
