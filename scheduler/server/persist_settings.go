package server

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

// Persistor interface for persisting scheduler settings and initializing the scheduler
// from its persisted settings
type Persistor interface {
	PersistSettings(settings *PersistedSettings) error
	LoadSettings() (*PersistedSettings, error)
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

func (p *nopPersistor) PersistSettings(settings *PersistedSettings) error {
	return nil
}

func (p *nopPersistor) LoadSettings() (*PersistedSettings, error) {
	return nil, nil
}

func (s *statefulScheduler) persistSettings() error {
	if s.persistor == nil {
		log.Infof("setting persistor is nil, scheduler will use default settings on restart")
		return nil
	}
	sa, ok := s.config.SchedAlg.(*LoadBasedAlg)
	if !ok {
		log.Errorf("not using load based scheduler, settings ignored")
		return nil
	}
	_, throttle := s.GetSchedulerStatus()
	ps := &PersistedSettings{
		ClassLoadPercents:               sa.getClassLoadPercents(),
		RequestorToClassMap:             sa.getRequestorToClassMap(),
		RebalanceMinimumDurationMinutes: int(sa.getRebalanceMinimumDuration().Minutes()),
		RebalanceThreshold:              sa.getRebalanceThreshold(),
		Throttle:                        throttle,
	}

	err := s.persistor.PersistSettings(ps)
	if err != nil {
		return fmt.Errorf("settings were not persisted, default scheduler settings will be used on next restart.%s", err)
	}
	return nil
}

func (s *statefulScheduler) loadSettings() {
	if s.persistor == nil {
		log.Info("no settings persistor provided, scheduler will use the default settings.")
		return
	}
	settings, err := s.persistor.LoadSettings()
	if err != nil {
		log.Errorf("error loading settings, scheduler will use the default settings. %s", err)
		return
	}
	if settings == nil {
		log.Infof("no persisted settings found. Scheduler will use default values")
		return
	}
	sa, ok := s.config.SchedAlg.(*LoadBasedAlg)
	if !ok {
		log.Errorf("not using load based scheduler, settings ignored")
		return
	}
	log.Info("loaded persisted settings")
	sa.setClassLoadPercents(settings.ClassLoadPercents)
	sa.setRequestorToClassMap(settings.RequestorToClassMap)
	sa.setRebalanceMinimumDuration(time.Duration(settings.RebalanceMinimumDurationMinutes) * time.Minute)
	sa.setRebalanceThreshold(settings.RebalanceThreshold)
	s.SetSchedulerStatus(settings.Throttle)
}
