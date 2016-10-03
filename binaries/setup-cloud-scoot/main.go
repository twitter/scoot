package main

import (
	"github.com/scootdev/scoot/tests/swarmtest"
	"time"
)

// Sets up a local swarm, then waits.
func main() {
	s := swarmtest.SwarmTest{}

	err := s.InitOptions(map[string]interface{}{
		"num_workers": 20,
		"num_jobs":    100,
		"timeout":     100 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	s.Wait = true
	s.Main()
}
