package bundlestore

import (
	"os"

	"github.com/twitter/scoot/ice"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/scootapi"
)

// Make a File Store based on the environment, or in temp if unset
func MakeFileStoreInEnvOrTemp(tmp *temp.TempDir) (*FileStore, error) {
	// if we're running as part of a swarm test, we want to share the store with other processes
	if d := os.Getenv(scootapi.BundlestoreEnvVar); d != "" {
		return MakeFileStore(d)
	}
	return MakeFileStoreInTemp(tmp)
}

func DefaultStore(store *FileStore) Store {
	return store
}

// Module returns a module that supports serving Bundlestore
func Module() ice.Module {
	return module{}
}

type module struct{}

// Install installs functions for serving Bundlestore
func (m module) Install(b *ice.MagicBag) {
	b.Put(MakeFileStoreInEnvOrTemp)
	b.Put(MakeServer)
	b.Put(DefaultStore)
}
