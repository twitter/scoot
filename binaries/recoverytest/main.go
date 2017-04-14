package main

import (
	"flag"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/scootdev/scoot/common/log/hooks"
	"github.com/scootdev/scoot/scootapi"
	"github.com/scootdev/scoot/tests/testhelpers"
)

func main() {
	log.AddHook(hooks.NewContextHook())

	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
	flag.Parse()

	level, err := log.ParseLevel(*logLevelFlag)
	if err != nil {
		log.Error(err)
		level = "info"
	}
	log.SetLevel(level)

	// RecoverTest Parameters
	numJobs := 20
	timeout := time.Minute

	var wg sync.WaitGroup
	scootClient := testhelpers.CreateScootClient(scootapi.DefaultSched_Thrift)

	// Initialize Local Cluster
	cluster1Cmds, err := testhelpers.CreateLocalTestCluster()
	if err != nil {
		log.Fatalf("Unexpected Error while Setting up Local Cluster %v", err)
	}
	defer cluster1Cmds.Kill()

	testhelpers.WaitForClusterToBeReady(scootClient)

	// Add a Bunch of Jobs to Scoot CloudExec
	log.Infof("Add Jobs to Scoot Cloud Exec")
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
			log.Fatalf("Error Occurred Waiting For Jobs to Complete.  %v", err)
		}
	}()

	// Wait for Jobs to Start Running and make some progress
	time.Sleep(1 * time.Second)

	log.Infof(
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

	log.Infof("Reviving Cluster")
	cluster2Cmds, err := testhelpers.CreateLocalTestCluster()
	if err != nil {
		log.Fatalf("Unexpected Error while Setting up Recovery Cluster %v", err)
	}
	defer cluster2Cmds.Kill()

	// Wait for all jobs to complete
	wg.Wait()
	log.Infof("All Jobs Completed Successfully")
}
