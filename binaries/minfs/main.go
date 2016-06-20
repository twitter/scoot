package main

import (
	"log"

	"github.com/scootdev/scoot/fs/minfuse"
)

func main() {
	minfuse.SetupLog()
	if opts, err := minfuse.InitFlags(); err != nil {
		log.Print(err)
		return
	} else {
		minfuse.Runfs(opts)
	}
}
