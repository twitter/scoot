package main

import (
	"flag"
	"github.com/scootdev/scoot/daemon/server"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/runner/execer/fake"
	"github.com/scootdev/scoot/runner/execer/os"
	"github.com/scootdev/scoot/runner/local"
	"log"
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
	r := local.NewSimpleRunner(ex)
	s, err := server.NewServer(r)
	if err != nil {
		log.Fatal("Cannot create Scoot server: ", err)
	}
	err = s.ListenAndServe()
	if err != nil {
		log.Fatal("Error serving Local Scoot: ", err)
	}
}
