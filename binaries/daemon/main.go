package main

import (
	"flag"
	"log"

	"github.com/scootdev/scoot/daemon/server"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/runner/execer/fake"
	"github.com/scootdev/scoot/runner/execer/os"
	"github.com/scootdev/scoot/runner/local"
	fakesnaps "github.com/scootdev/scoot/snapshots/fake"
)

var execerType = flag.String("execer_type", "sim", "execer type; os or sim")

// A Scoot Daemon server.
func main() {
	flag.Parse()
	var ex execer.Execer
	switch *execerType {
	case "sim":
		ex = fake.NewSimExecer(nil)
	case "os":
		ex = os.NewExecer()
	default:
		log.Fatalf("Unknown execer type %v", *execerType)
	}
	saver, err := local.NewSaver()
	if err != nil {
		log.Fatal("Cannot create Saver: ", err)
	}
	r := local.NewSimpleRunner(ex, fakesnaps.MakeInvalidCheckouter(), saver)
	s, err := server.NewServer(r)
	if err != nil {
		log.Fatal("Cannot create Scoot server: ", err)
	}
	err = s.ListenAndServe()
	if err != nil {
		log.Fatal("Error serving Local Scoot: ", err)
	}
}
