package scheduler

import (
	log "github.com/sirupsen/logrus"
	"math"
	"sync"
	"time"

	"github.com/twitter/scoot/saga"
	"github.com/twitter/scoot/sched"
)

// recovers all active sagas from the specified SagaCoordinator with ForwardRecovery.
// ActiveSagas are recovered in parallel and are added to the addJobCh to be rescheduled
// This method returns once all activeSagas have been successfully recovered.
func recoverJobs(sc saga.SagaCoordinator, addJobCh chan jobAddedMsg) {
	log.Infof("INFO: Recovering Sagas")

	recoveryActiveSagaAttempts := 0
	activeSagas, err := sc.Startup()
	for err != nil {
		// Retry until we succeed,  This has to eventually succeed
		// for us to make progress.
		// TODO: Add metrics for failure rate, this would be something we should alert on.
		recoveryActiveSagaAttempts++
		log.Infof("ERROR: occurred getting ActiveSagas from SagaLog %v", err)

		delay := calculateExponentialBackoff(recoveryActiveSagaAttempts, time.Duration(1)*time.Minute)
		time.Sleep(delay)
		activeSagas, err = sc.Startup()
	}

	// if there are no activeSagas return
	if activeSagas == nil {
		return
	}

	log.Infof("DEBUG: Recovering Active Sagas %+v", activeSagas)

	var wg sync.WaitGroup
	wg.Add(len(activeSagas))

	// Spin off separate go routines for each active saga.  This way we can
	// do recovery in parallel by making multiple requests
	// TODO: limit max parallel requests to RecoverSagaState?
	for _, sagaId := range activeSagas {

		go func(sagaId string) {
			defer wg.Done()
			activeSaga := recoverSaga(sc, sagaId)
			if activeSaga != nil {
				job, err := sched.DeserializeJob(activeSaga.GetState().Job())
				if err != nil {
					// TODO: Increment counter? A breaking change was made
					// if this happens or data was corrupted.
					log.Infof("Error: Could not deserialize Job for Saga %v, error: %v", sagaId, err)
					return
				}

				log.Infof("INFO: Rescheduling Saga %v", sagaId)
				// reschedule saga
				addJobCh <- jobAddedMsg{
					job:  job,
					saga: activeSaga,
				}
			}

		}(sagaId)
	}

	wg.Wait()
}

// Attempts to recover the specified saga from the provided SagaCoordinator
// If the specified Saga exists and is still Active (no End Saga Message Logged) then
// then a Saga will be returned.  If it does not nil will be returned.
// If a Fatal Error occurrs while recovering the saga nil will be returned.
// If a Retryable Error occurs, like SagaLog temporarily unavailable recovery will
// be retried until it succeeds
func recoverSaga(sc saga.SagaCoordinator, sagaId string) *saga.Saga {
	recoverSagaStateAttempts := 0
	activeSaga, err := sc.RecoverSagaState(sagaId, saga.ForwardRecovery)
	for err != nil {
		// TODO: add metrics for failure rate, this would be something we should alert on

		// check if recoverable error
		if saga.FatalErr(err) {
			// TODO: add metrics for fatal failure rate, this would be something we should alert on, this is a bad bug
			// if we can't recover the saga from the long, means something is very wrong.
			log.Infof("ERROR: Fatal Error occurred recovering saga %v, with error: %v, skipping recovery for this saga", sagaId, err)
			err = nil
			activeSaga = nil
		} else {
			// Recovering SagaState must eventually succeed if it doesn't continue to retry with
			// exponential backoff.
			recoverSagaStateAttempts++
			log.Infof("ERROR: occurred recovering Saga %v, from SagaLog %v", sagaId, err)

			delay := calculateExponentialBackoff(recoverSagaStateAttempts, time.Duration(1)*time.Minute)
			time.Sleep(delay)
			activeSaga, err = sc.RecoverSagaState(sagaId, saga.ForwardRecovery)
		}
	}

	// If Saga doesn't exist anymore skip it
	// This could happen because a Saga got added to active index, but failed to
	// log successfully.  In this case Starting the Saga will have failed.
	if activeSaga == nil {
		log.Infof("DEBUG: Saga doesn't exist %v", sagaId)
		return nil
	}

	// If Saga is Completed, discard
	// This could happen because Active Index did not get updated successfully,
	// even if the saga has been completed.
	if activeSaga.GetState().IsSagaCompleted() {
		log.Infof("DEBUG: Saga Already Completed %v", sagaId)
		return nil
	}

	return activeSaga
}

// calculates the amount of time to wait before retrying
// based on the number of attempts tried or the maximum delay specified
// returns the duration to wait
func calculateExponentialBackoff(attempt int, maxDelay time.Duration) time.Duration {
	var c = float64(attempt)
	delaySeconds := (math.Pow(2, c) - float64(1)) / float64(2)
	delayTime := time.Duration(delaySeconds*1000) * time.Millisecond

	if delayTime > maxDelay {
		return maxDelay
	} else {
		return delayTime
	}
}
