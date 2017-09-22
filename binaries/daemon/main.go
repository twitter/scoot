package main

import (
	"flag"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/daemon/server"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/runner/execer"
	"github.com/twitter/scoot/runner/execer/execers"
	os_exec "github.com/twitter/scoot/runner/execer/os"
	"github.com/twitter/scoot/runner/runners"
	"github.com/twitter/scoot/snapshot/snapshots"
)

// A Scoot Daemon server.
func main() {
	log.AddHook(hooks.NewContextHook())

	execerType := flag.String("execer_type", "sim", "execer type; os or sim")
	qLen := flag.Int("test_q_len", 1000000, "queue length for testing")
	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
	flag.Parse()

	level, err := log.ParseLevel(*logLevelFlag)
	if err != nil {
		log.Error(err)
		return
	}
	log.SetLevel(level)

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
	r := runners.NewQueueRunner(ex, filer, nil, outputCreator, tmp, *qLen, nil)
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
