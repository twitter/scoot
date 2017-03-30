package main

import (
	"github.com/scootdev/scoot/common/log"

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
