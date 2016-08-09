package main

import (
	"flag"
	"log"
	"sync"

	"github.com/scootdev/scoot/sched/scheduler"
	"github.com/scootdev/scoot/scootapi/server"
)

var addr = flag.String("addr", "localhost:9090", "Bind address for api server.")
var cfgFile = flag.String("cfg", "", "Path to sched overlay config file.")

func main() {
	log.Println("Starting Cloud Scoot API Server & Scheduler")
	flag.Parse()

	cfg, _ := NewSchedConfig(*cfgFile)
	sched, _ := cfg.ToScheduler()
	handler, _ := cfg.ToServerHandler()

	var wg sync.WaitGroup
	wg.Add(2)

	// Start API Server
	go func() {
		log.Println("Starting API Server")
		defer wg.Done()
		err := server.Serve(handler, *addr, cfg.TransportFactory, cfg.ProtocolFactory)
		if err != nil {
			log.Fatal("Error serving Scoot API: ", err)
		}
	}()

	// Go Routine which takes data from work queue and schedules it
	go func() {
		log.Println("Starting Scheduler")
		defer wg.Done()
		scheduler.GenerateWork(sched, cfg.QueueImpl.Chan())
	}()

	wg.Wait()
}
