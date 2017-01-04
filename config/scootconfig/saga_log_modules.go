package scootconfig

import (
	"github.com/scootdev/scoot/ice"
	"github.com/scootdev/scoot/saga"
	"github.com/scootdev/scoot/saga/sagalogs"
)

// InMemorySagaLog struct is used by goice to create an InMemory instance
// of the SagaLog interface.
type InMemorySagaLogConfig struct {
	Type string
}

// Adds the InMemorySagaLog Create function to the goice MagicBag
func (c *InMemorySagaLogConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

// Creates an instance of an InMemorySagaLog
func (c *InMemorySagaLogConfig) Create() saga.SagaLog {
	return sagalogs.MakeInMemorySagaLog()
}

// FileSagaLogConfig struct is used by goice to create a FileSagaLog
// instance of the SagaLog interface
// Directory specifies the name of the directory to store
// Sagalog files in.
type FileSagaLogConfig struct {
	Type      string
	Directory string
}

// Adds the FileSagaLogConfig Create function to the goice MagicBag
func (c *FileSagaLogConfig) Install(bag *ice.MagicBag) {
	bag.Put(c.Create)
}

// Creates an instance of the FileSagaLog
func (c *FileSagaLogConfig) Create() (saga.SagaLog, error) {
	return sagalogs.MakeFileSagaLog(c.Directory)
}
