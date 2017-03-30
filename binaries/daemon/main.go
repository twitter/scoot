package main

import (
	"flag"
	log "github.com/Sirupsen/logrus"
	"time"

	"github.com/scootdev/scoot/daemon/server"
	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner/execer"
	"github.com/scootdev/scoot/runner/execer/execers"
	os_exec "github.com/scootdev/scoot/runner/execer/os"
	"github.com/scootdev/scoot/runner/runners"
	"github.com/scootdev/scoot/snapshot/snapshots"
)

var execerType = flag.String("execer_type", "sim", "execer type; os or sim")
var qLen = flag.Int("test_q_len", 1000000, "queue length for testing")

// A Scoot Daemon server.
func main() {
	flag.Parse()
	var ex execer.Execer
	switch *execerType {
	case "sim":
		ex = execers.NewSimExecer()
	case "os":
		ex = os_exec.NewExecer()
	default:
		log.Fatalf("Unknown execer type %v", *execerType)
	}

	tempDir, err := temp.TempDirDefault()
	if err != nil {
		log.Fatal("error creating temp dir: ", err)
	}
	//defer os.RemoveAll(tempDir.Dir) //TODO: this may become necessary if we start testing with larger snapshots.

	tmp, err := temp.NewTempDir("", "daemon")
	if err != nil {
		log.Fatal("Cannot create tmp dir: ", err)
	}

	outputCreator, err := runners.NewHttpOutputCreator(tempDir, "")
	if err != nil {
		log.Fatal("Cannot create OutputCreator: ", err)
	}
	filer := snapshots.MakeTempFiler(tempDir)
	r := runners.NewQueueRunner(ex, filer, outputCreator, tmp, *qLen)
	h := server.NewHandler(r, filer, 50*time.Millisecond)
	s, err := server.NewServer(h)
	if err != nil {
		log.Fatal("Cannot create Scoot server: ", err)
	}
	err = s.ListenAndServe()
	if err != nil {
		log.Fatal("Error serving Scoot Daemon: ", err)
	}
}
