// +build !unit
// +build integration

package swarmtest

import (
	"fmt"
	"testing"
	"time"
)

func Test_RunSwarmTest(t *testing.T) {
	s := SwarmTest{}
	err := s.InitOptions(map[string]interface{}{
		"num_workers": 10,
		"num_jobs":    10,
		"timeout":     10 * time.Second,
	})
	if err != nil {
		t.Error("Error Initializing Swarm Test")
	}

	err = s.RunSwarmTest()
	fmt.Println(err)
	if err != nil {
		t.Error("Swarm Test did not complete successfully")
	}
}
