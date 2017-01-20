package testhelpers

import (
	"log"
	"time"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
	"github.com/scootdev/scoot/scootapi/setup"
)

// Spins up a new Local Test Cluster in a separate
// goroutine.  Returns the associated Cmds.  To Kill
// the cluster run Kill() on return Cmds
func CreateLocalTestCluster() (*setup.Cmds, error) {
	tmp, err := temp.NewTempDir("", "localTestCluster")
	if err != nil {
		return nil, err
	}

	clusterCmds := setup.NewSignalHandlingCmds(tmp)
	builder := setup.NewGoBuilder(clusterCmds)

	go func() {
		strategies := map[string]setup.SchedulerStrategy{
			"local.local": setup.NewLocalLocal("", builder, clusterCmds),
		}
		setup.Main(clusterCmds, strategies, "local.local", []string{})
	}()

	return clusterCmds, nil
}

// Blocks until the cluster is ready, by pinging the GetStatus Api
// Until a successful response is returned.
func WaitForClusterToBeReady(client scoot.CloudScoot) {
	status, err := client.GetStatus("testJobId")
	log.Printf("Waiting for Cluster Status: %+v, Error: %v", status, err)

	for err != nil {
		time.Sleep(500 * time.Millisecond)
		status, err = client.GetStatus("testJobId")
		log.Printf("Waiting for Cluster Status: %+v, Error: %v", status, err)
	}

	return
}
