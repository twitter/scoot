package main

import (
	smoketest "github.com/scootdev/scoot/scripts/common"
)

func main() {
	s := smoketest.SmokeTest{}
	opts, err := s.InitOptions()
	if err != nil {
		panic(err)
	}
	s.Main(opts)
}
