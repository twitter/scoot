package main

import (
	"github.com/scootdev/scoot/local/protocol"
	"github.com/scootdev/scoot/local/server"
	"github.com/scootdev/scoot/runner/local"
	"log"
)

// A Local Scoot server.
func main() {
	scootdir, err := protocol.LocateScootDir()
	if err != nil {
		log.Fatal("Error locating Scoot instance: ", err)
	}
	r := local.NewSimpleRunner(nil)
	s, err := server.NewServer(r)
	if err != nil {
		log.Fatal("Cannot create Scoot server: ", err)
	}
	err = server.Serve(s, scootdir)
	if err != nil {
		log.Fatal("Error serving Local Scoot: ", err)
	}
}
