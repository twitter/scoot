package main

import (
	"github.com/scootdev/scoot/local/client/cli"
	"github.com/scootdev/scoot/local/client/conn"
	"log"
)

// A Scoot command-line client
func main() {
	dialer, err := conn.UnixDialer()
	if err != nil {
		log.Fatal("Cannot determine Local Scoot address", err)
	}
	cli, err := cli.NewCliClient(conn.NewCachingDialer(dialer))
	if err != nil {
		log.Fatal("Cannot initalize Local Scoot CLI: ", err)
	}
	err = cli.Exec()
	if err != nil {
		log.Fatal("error running scootcl ", err)
	}
}
