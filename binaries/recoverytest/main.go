package main

import (
	log "github.com/scootdev/scoot/common/logger"
	"sync"
	"time"

	"github.com/scootdev/scoot/scootapi"
	"github.com/scootdev/scoot/tests/testhelpers"
)

func main() {

	// RecoverTest Parameters
	numJobs := 20
	timeout := 40 * time.Second

	var wg sync.WaitGroup
	scootClient := testhelpers.CreateScootClient(scootapi.DefaultSched_Thrift)

	// Initialize Local Cluster
	cluster1Cmds, err := testhelpers.CreateLocalTestCluster()
	if err != nil {
		log.Crit("Unexpected Error while Setting up Local Cluster %v", err)
	}
	defer cluster1Cmds.Kill()

	testhelpers.WaitForClusterToBeReady(scootClient)

	// Add a Bunch of Jobs to Scoot CloudExec
	log.Info("Add Jobs to Scoot Cloud Exec")
	jobIds := make([]string, 0, numJobs)
	for i := 0; i < numJobs; i++ {
		jobIds = append(jobIds, testhelpers.StartJob(scootClient, testhelpers.GenerateJob(-1, "")))
	}

	// Wait for jobs to complete check in a separate go routine
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := testhelpers.WaitForJobsToCompleteAndLogStatus(jobIds, scootClient, timeout)
		if err != nil {
			log.Crit("Error Occurred Waiting For Jobs to Complete.  %v", err)
		}
	}()

	// Wait for Jobs to Start Running and make some progress
	time.Sleep(1 * time.Second)

	log.Info(
		`-------------------------------KILLING CLUSTER-------------------------------
                                     ________________
                            ____/ (  (    )   )  \___
                           /( (  (  )   _    ))  )   )\
                         ((     (   )(    )  )   (   )  )
                       ((/  ( _(   )   (   _) ) (  () )  )
                      ( (  ( (_)   ((    (   )  .((_ ) .  )_
                     ( (  )    (      (  )    )   ) . ) (   )
                    (  (   (  (   ) (  _  ( _) ).  ) . ) ) ( )
                    ( (  (   ) (  )   (  ))     ) _)(   )  )  )
                   ( (  ( \ ) (    (_  ( ) ( )  )   ) )  )) ( )
                    (  (   (  (   (_ ( ) ( _    )  ) (  )  )   )
                   ( (  ( (  (  )     (_  )  ) )  _)   ) _( ( )
                    ((  (   )(    (     _    )   _) _(_ (  (_ )
                     (_((__(_(__(( ( ( |  ) ) ) )_))__))_)___)
                     ((__)        \\||lll|l||///          \_))
                              (   /(/ (  )  ) )\   )
                            (    ( ( ( | | ) ) )\   )
                             (   /(| / ( )) ) ) )) )
                           (     ( ((((_(|)_)))))     )
                            (      ||\(|(|)|/||     )
                          (        |(||(||)||||        )
                            (     //|/l|||)|\\ \     )
                          (/ / //  /|//||||\\  \ \  \ _)
  -------------------------------------------------------------------------------`)
	cluster1Cmds.Kill()

	log.Info("Reviving Cluster")
	cluster2Cmds, err := testhelpers.CreateLocalTestCluster()
	if err != nil {
		log.Crit("Unexpected Error while Setting up Recovery Cluster %v", err)
	}
	defer cluster2Cmds.Kill()

	// Wait for all jobs to complete
	wg.Wait()
	log.Info("All Jobs Completed Successfully")
}
