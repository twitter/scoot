package main

import (
	"flag"
	"log"

	"github.com/scootdev/scoot/daemon/server"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/runner/execer/execers"
	"github.com/scootdev/scoot/runner/execer/os"
	"github.com/scootdev/scoot/runner/local"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

var execerType = flag.String("execer_type", "sim", "execer type; os or sim")

// A Scoot Daemon server.
func main() {
	flag.Parse()
	var ex execer.Execer
	switch *execerType {
	case "sim":
		ex = execers.NewSimExecer(nil)
	case "os":
		ex = os.NewExecer()
	default:
		log.Fatalf("Unknown execer type %v", *execerType)
	}
	outputCreator, err := local.NewOutputCreator()
	if err != nil {
		log.Fatal("Cannot create OutputCreator: ", err)
	}
	r := local.NewSimpleRunner(ex, snapshots.MakeInvalidCheckouter(), outputCreator)
	s, err := server.NewServer(r)
	if err != nil {
		log.Fatal("Cannot create Scoot server: ", err)
	}
	err = s.ListenAndServe()
	if err != nil {
		log.Fatal("Error serving Local Scoot: ", err)
	}
}
