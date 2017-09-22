package testhelpers

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/scootapi/gen-go/scoot"
	"github.com/twitter/scoot/scootapi/setup"
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
		strategy := `local.local.{"SchedulerConfig": {"DefaultTaskTimeoutMs": 1000, "RunnerOverheadMs": 0}}`
		sched := map[string]setup.SchedulerStrategy{
			strategy: setup.NewLocalLocal(&setup.WorkerConfig{LogLevel: log.InfoLevel}, builder, clusterCmds),
		}
		api := map[string]setup.ApiStrategy{
			"local": setup.NewLocal(&setup.ApiConfig{LogLevel: log.InfoLevel}, builder, clusterCmds),
		}
		strategies := &setup.Strategies{Sched: sched, SchedStrategy: strategy, Api: api, ApiStrategy: "local"}
		setup.Main(clusterCmds, strategies, []string{})
	}()

	return clusterCmds, nil
}

// Blocks until the cluster is ready, by pinging the GetStatus Api
// Until a successful response is returned.
func WaitForClusterToBeReady(client scoot.CloudScoot) {
	status, err := client.GetStatus("testJobId")
	log.Infof("Waiting for Cluster Status: %+v, Error: %v", status, err)

	for err != nil {
		time.Sleep(500 * time.Millisecond)
		status, err = client.GetStatus("testJobId")
		log.Infof("Waiting for Cluster Status: %+v, Error: %v", status, err)
	}

	return
}
