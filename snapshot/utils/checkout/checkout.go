package main

// A simple utility to checkout (i.e., copy) files in a Snapshot.
// It can use either Scoot Snapshots or Go's OS library as a backend.
// This lets us make sure Scoot's overhead is comparable to raw OS access.

import (
	"flag"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/twitter/scoot/snapshot"
)

type checkoutContext struct {
	srcRoot string
	dstRoot string
	snap    snapshot.Snapshot
}

func copyFiles(ctx *checkoutContext, relPath string) {
	var isDir bool
	if useSnapshot {
		fi, err := ctx.snap.Stat(relPath)
		if err != nil {
			log.Info("Couldn't Stat", err, relPath)
			return
		}
		isDir = fi.IsDir()
	} else {
		fi, err := os.Stat(path.Join(ctx.srcRoot, relPath))
		if err != nil {
			log.Info("Couldn't stat", err, relPath)
			return
		}
		isDir = fi.IsDir()
	}
	dstPath := path.Join(ctx.dstRoot, relPath)
	if !isDir {
		var bs []byte
		var err error

		var r io.Reader

		// TODO(dbentley): copy contents
		if useSnapshot {
			f, err := ctx.snap.Open(relPath)
			if err != nil {
				log.Info("Couldn't open", err, relPath)
				return
			}
			defer f.Close()
			r = snapshot.MakeCursor(f)
		} else {
			f, err := os.Open(path.Join(ctx.srcRoot, relPath))
			if err != nil {
				log.Info("Couldn't open", err, relPath)
				return
			}
			defer f.Close()
			r = f
		}
		bs, err = ioutil.ReadAll(r)
		if err != nil {
			log.Info("Couldn't read", err)
		}
		err = ioutil.WriteFile(dstPath, bs, 0777)
		if err != nil {
			log.Info("Couldn't write", err, relPath, dstPath)
		}
		return
	}
	os.MkdirAll(dstPath, 0777)
	var children []string
	if useSnapshot {
		childDirents, err := ctx.snap.Readdirents(relPath)
		if err != nil {
			log.Info("Couldn't ReadDir", err, relPath)
			return
		}
		children = make([]string, len(childDirents))
		for idx, child := range childDirents {
			children[idx] = child.Name
		}
	} else {
		f, err := os.Open(path.Join(ctx.srcRoot, relPath))
		if err != nil {
			log.Info("Couldn't open", err, relPath)
			return
		}
		defer f.Close()
		children, err = f.Readdirnames(0)
		if err != nil {
			log.Info("Couldn't Readdirnames", err, relPath)
			return
		}
	}
	for _, child := range children {
		copyFiles(ctx, path.Join(relPath, child))
	}
}

var srcRoot string
var dstRoot string
var useSnapshot bool

func init() {
	flag.StringVar(&srcRoot, "src", "", "root of source directory")
	flag.StringVar(&dstRoot, "dst", "", "root of destination directory")
	flag.BoolVar(&useSnapshot, "use_snapshot", false, "whether to use snapshot interface")
}

func main() {
	flag.Parse()
	if srcRoot == "" || dstRoot == "" {
		log.Fatal("-src and -dst must both be set")
	}
	snaps := snapshot.NewFileBackedSnapshots(srcRoot)
	snap, err := snaps.Get("foo")
	if err != nil {
		log.Fatal("Invalid ID \"foo\":", err)
	}
	ctx := checkoutContext{srcRoot: srcRoot, snap: snap, dstRoot: dstRoot}
	copyFiles(&ctx, "")
	log.Infof("Checked out\n")
}
