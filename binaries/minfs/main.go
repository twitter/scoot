package main

import (
	log "github.com/inconshreveable/log15"

	"github.com/scootdev/scoot/fs/minfuse"
)

func main() {
	minfuse.SetupLog()
	if opts, err := minfuse.InitFlags(); err != nil {
		log.Info(err.Error())
		return
	} else {
		minfuse.Runfs(opts)
	}
}
