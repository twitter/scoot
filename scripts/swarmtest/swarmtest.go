package main

import (
	swarmtest "github.com/scootdev/scoot/scripts/common"
)

func main() {
	s := swarmtest.SwarmTest{}
	err := s.InitOptions(nil)
	if err != nil {
		panic(err)
	}
	s.Main()
}
