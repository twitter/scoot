package swarmtest

import (
	"fmt"
	"testing"
)

// This test will trigger the race condition because we wait on processes
// and then kill them in different go routines.  This is ok though!
func Test_RunSwarmTest(t *testing.T) {
	s := SwarmTest{}
	err := s.InitOptions(nil)
	if err != nil {
		t.Error("Error Initializing Swarm Test")
	}

	err = s.RunSwarmTest()
	fmt.Println(err)
	if err != nil {
		t.Error("Swarm Test did not complete successfully")
	}
}
