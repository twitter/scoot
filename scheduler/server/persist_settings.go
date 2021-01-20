package server

import (
	"time"

	log "github.com/sirupsen/logrus"
)

// Persistor interface for persisting scheduler settings and initializing the scheduler
// from its persisted settings
type Persistor interface {
	PersistSettings(s Scheduler)
	LoadSettings(s Scheduler)
}

// PersistedSettings the persisted scheduler settings structure for encoding/decoding as json
type PersistedSettings struct {
	ClassLoadPercents               map[string]int32  `json:"classLoadPercents"`
	RequestorToClassMap             map[string]string `json:"requestorToClassMap"`
	RebalanceMinimumDurationMinutes int               `json:"rebalanceMinimumDurationMinutes"`
	RebalanceThreshold              int               `json:"rebalanceThreshold"`
	Throttle                        int               `json:"throttle"`
}

// nopPersistor provides nop implementations of persist and load functions
type nopPersistor struct{}

func (p *nopPersistor) PersistSettings(s Scheduler) {
	return
}

func (p *nopPersistor) LoadSettings(s Scheduler) {
	return
}

func (s *statefulScheduler) LoadPersistedSettings(settings PersistedSettings) {
	sa, ok := s.config.SchedAlg.(*LoadBasedAlg)
	if !ok {
		log.Errorf("not using load based scheduler, settings ignored")
		return
	}
	sa.setClassLoadPercents(settings.ClassLoadPercents)
	sa.setRequestorToClassMap(settings.RequestorToClassMap)
	sa.setRebalanceMinimumDuration(time.Duration(settings.RebalanceMinimumDurationMinutes) * time.Minute)
	sa.setRebalanceThreshold(settings.RebalanceThreshold)
	s.SetSchedulerStatus(settings.Throttle)
}
