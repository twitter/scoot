package main

import (
	"log"
	"sync"
	"time"

	"github.com/scootdev/scoot/tests/testhelpers"
)

func main() {

	// RecoverTest Parameters
	numJobs := 20
	timeout := 20 * time.Second

	var wg sync.WaitGroup
	scootClient := testhelpers.CreateScootClient("localhost:9090")

	// Initialize Local Cluster
	cluster1Cmds, err := testhelpers.CreateLocalTestCluster()
	if err != nil {
		log.Fatalf("Unexpected Error while Setting up Local Cluster %v", err)
	}
	defer cluster1Cmds.Kill()

	testhelpers.WaitForClusterToBeReady(scootClient)

	// Add a Bunch of Jobs to Scoot CloudExec
	log.Printf("Add Jobs to Scoot Cloud Exec")
	jobIds := make([]string, 0, numJobs)
	for i := 0; i < numJobs; i++ {
		id, err := testhelpers.GenerateAndStartJob(scootClient, -1, testhelpers.DefaultSnapshotCmd())
		if err != nil {
			log.Fatalf("Could not schedule Jobs Error: %v", err)
		} else {
			jobIds = append(jobIds, id)
		}
	}

	// Wait for jobs to complete check in a separate go routine
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := testhelpers.WaitForJobsToCompleteAndLogStatus(jobIds, scootClient, timeout)
		if err != nil {
			log.Fatalf("Error Occurred Waiting For Jobs to Complete.  %v", err)
		}
	}()

	// Wait for Jobs to Start Running and make some progress
	time.Sleep(1 * time.Second)

	log.Printf(
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

	log.Printf("Reviving Cluster")
	cluster2Cmds, err := testhelpers.CreateLocalTestCluster()
	if err != nil {
		log.Fatalf("Unexpected Error while Setting up Recovery Cluster %v", err)
	}
	defer cluster2Cmds.Kill()

	// Wait for all jobs to complete
	wg.Wait()
	log.Printf("All Jobs Completed Successfully")
}
