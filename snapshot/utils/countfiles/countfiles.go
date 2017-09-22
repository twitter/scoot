package main

// A simple utility to count files in a Snapshot.
// It can use either Scoot Snapshot or Go's OS library as a backend.
// This lets us make sure Scoot's overhead is comparable to raw OS access.

import (
	"flag"
	log "github.com/sirupsen/logrus"
	"os"
	"path"

	"github.com/twitter/scoot/snapshot"
)

type countContext struct {
	root string
	snap snapshot.Snapshot
}

func countFiles(ctx *countContext, count *int, relPath string) {
	var isDir bool
	if useSnapshot {
		fi, err := ctx.snap.Stat(relPath)
		if err != nil {
			log.Info("Couldn't Stat", err, relPath)
			return
		}
		isDir = fi.IsDir()
	} else {
		fi, err := os.Stat(path.Join(ctx.root, relPath))
		if err != nil {
			log.Info("Couldn't stat", err, relPath)
			return
		}
		isDir = fi.IsDir()
	}
	*count = *count + 1
	if !isDir {
		return
	}
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
		f, err := os.Open(path.Join(ctx.root, relPath))
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
		countFiles(ctx, count, path.Join(relPath, child))
	}
}

var root string
var useSnapshot bool

func init() {
	flag.StringVar(&root, "root", "", "root of filesystem to count")
	flag.BoolVar(&useSnapshot, "use_snapshot", false, "whether to use snapshot interface")
}

func main() {
	flag.Parse()
	if root == "" {
		log.Fatal("-root not set")
	}
	snaps := snapshot.NewFileBackedSnapshots(root)
	snap, err := snaps.Get("foo")
	if err != nil {
		log.Fatal("Invalid ID \"foo\":", err)
	}
	ctx := countContext{root: root, snap: snap}
	var fileCount int
	countFiles(&ctx, &fileCount, "")
	log.Infof("Counted %v files", fileCount)
}
