package main

import (
	swarmtest "github.com/scootdev/scoot/scripts/common"
)

func main() {
	s := swarmtest.SwarmTest{}
	opts, err := s.InitOptions()
	if err != nil {
		panic(err)
	}
	s.Main(opts)
}
