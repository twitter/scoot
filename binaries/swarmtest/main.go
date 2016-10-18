package main

import (
	"github.com/scootdev/scoot/tests/swarmtest"
	"time"
)

// Runs an end to end integration test with work being scheduled
// via the ScootApi placed on the WorkQueue, Dequeued by the Scheduler
// and ran on local instances of Workers.
func main() {
	s := swarmtest.SwarmTest{}
	s.DoCompile = true

	err := s.InitOptions(map[string]interface{}{
		"num_workers": 1,
		"num_jobs":    1,
		"timeout":     100 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	s.Main()
}
