package server

// Persistor interface for persisting scheduler settings and initializing the scheduler
// from its persisted settings
type Persistor interface {
	PersistSettings(s Scheduler)
	LoadSettings(s Scheduler)
}

// nopPersistor provides nop implementations of persist and load functions
type nopPersistor struct{}

func (p *nopPersistor) PersistSettings(s Scheduler) {
	return
}

func (p *nopPersistor) LoadSettings(s Scheduler) {
	return
}
