package main

import (
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	_ "net/http/pprof"

	"github.com/scootdev/fuse"
	"github.com/scootdev/scoot/fs/min"
	"github.com/scootdev/scoot/fs/minfuse"
	"github.com/scootdev/scoot/snapshots"
)

var src = flag.String("src_root", "", "source directory to mirror")
var mountpoint = flag.String("mountpoint", "", "directory to mount at")

var trace = flag.Bool("trace", false, "whether to trace execution")

var serveStrategy = flag.String("serve_strategy", "",
	"Options are any of async|sync;serial|threadpool;readahead_mb=N. Default is 'async;serial;readahead_mb=4'")

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	flag.Parse()

	if *src == "" || *mountpoint == "" {
		log.Fatal("Both src_root and mountpoint must be set")
	}

	// Note on serial vs threadpool option:
	// Serial means that no locking will be done at all and we will serve from a single goroutine.
	// Threadpool means that we will lock where necessary and serve from multiple goroutines.
	async := true
	threadUnsafe := true
	maxReadahead := uint32(4 * 1024 * 1024)
	strategyList := strings.Split(*serveStrategy, ";")
	for _, strategy := range strategyList {
		switch {
		case strategy == "async":
			async = true
		case strategy == "sync":
			async = false
		case strategy == "serial":
			threadUnsafe = true
		case strategy == "threadpool":
			threadUnsafe = false
		case strings.HasPrefix(strategy, "readahead_mb="):
			readaheadStr := strings.Split(strategy, "=")[1]
			if readahead, err := strconv.ParseFloat(readaheadStr, 32); err == nil {
				maxReadahead = uint32(readahead * 1024 * 1024)
				break
			}
			fallthrough
		default:
			log.Fatal("Unrecognized strategy", strategy)
		}
	}
	log.Print("Options(Async,ThreadUnsafe,MaxReadahead):", async, threadUnsafe, maxReadahead)

	snap := snapshots.NewFileBackedSnapshot(*src, "only")
	minfs := minfuse.NewSlimMinFs(snap)

	if *trace {
		fuse.Trace = true
		snapshots.Trace = true
		min.Trace = true
	}

	options := []fuse.MountOption{
		fuse.DefaultPermissions(),
		fuse.MaxReadahead(maxReadahead),
		fuse.FSName("slimfs"),
		fuse.Subtype("fs"),
		fuse.VolumeName("slimfs"),
	}
	if async {
		options = append(options, fuse.AsyncRead())
	}

	log.Print("About to Mount")
	fuse.Unmount(*mountpoint)
	conn, err := fuse.Mount(*mountpoint, fuse.MakeAlloc(), options...)
	if err != nil {
		log.Fatal("Couldn't mount", err)
	}

	var done chan error
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigchan
		log.Printf("Canceling")
		if done != nil {
			done <- errors.New("Caller canceled")
		}
	}()

	go func() {
		log.Println("pprof exit: ", http.ListenAndServe("localhost:6060", nil))
	}()

	defer func() {
		if err := fuse.Unmount(*mountpoint); err != nil {
			log.Printf("error in call to Unmount(%s): %s", *mountpoint, err)
			return
		}
		log.Printf("called Umount on %s", *mountpoint)
	}()

	// Serve returns immediately and we wait for the first entry from the done channel before exiting main.
	// We only care about the first error from either the signal handler or from the first serve thread to return.
	// Exiting main will cause the remaining read threads to exit.
	log.Print("About to Serve")
	done = min.Serve(conn, minfs, threadUnsafe)
	err = <-done
	log.Printf("Returning (might take a few seconds), err=%v", err)
}
