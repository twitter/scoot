package main

import (
	"github.com/scootdev/scoot/local/protocol"
	"github.com/scootdev/scoot/local/server"
	"log"
)

// A Local Scoot server.
func main() {
	scootdir, err := protocol.LocateScootDir()
	if err != nil {
		log.Fatal("Error locating Scoot instance: ", err)
	}
	s, err := server.NewServer()
	if err != nil {
		log.Fatal("Cannot create Scoot server: ", err)
	}
	err = server.Serve(s, scootdir)
	if err != nil {
		log.Fatal("Error serving Local Scoot: ", err)
	}
}
