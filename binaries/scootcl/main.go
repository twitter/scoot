package main

import (
	"github.com/scootdev/scoot/daemon/client/cli"
	"github.com/scootdev/scoot/daemon/client/conn"
	"log"
)

// A Local Scoot command-line client
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
