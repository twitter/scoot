package bundlestore

import (
	"net"
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/common/endpoints"
	"github.com/twitter/scoot/config/jsonconfig"
	"github.com/twitter/scoot/ice"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/scootapi"
	"github.com/twitter/scoot/snapshot/store"
)

type servers struct {
	http *endpoints.TwitterServer
	grpc bazel.GRPCServer
}

func makeServers(h *endpoints.TwitterServer, g bazel.GRPCServer) servers {
	return servers{http: h, grpc: g}
}

// Make a File Store based on the environment, or in temp if unset
func MakeFileStoreInEnvOrTemp(tmp *temp.TempDir) (*store.FileStore, error) {
	// if we're running as part of a swarm test, we want to share the store with other processes
	if d := os.Getenv(BundlestoreDirEnvVar); d != "" {
		return store.MakeFileStore(d)
	}
	return store.MakeFileStoreInTemp(tmp)
}

func DefaultStore(store *store.FileStore) store.Store {
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

// Creates a MagicBag for a default bundlestore server and returns it
func Defaults() *ice.MagicBag {
	bag := ice.NewMagicBag()
	bag.PutMany(
		func(h *endpoints.TwitterServer, g bazel.GRPCServer) servers {
			return makeServers(h, g)
		},

		func() (net.Listener, error) {
			return net.Listen("tcp", scootapi.DefaultApiBundlestore_GRPC)
		},

		func(s *Server) bazel.GRPCServer {
			return s.casServer
		},
	)
	return bag
}

func RunServer(bag *ice.MagicBag, schema jsonconfig.Schema, config []byte) {
	// Parse Config
	log.Info("bundlestore RunServer(), config is:", string(config))
	mod, err := schema.Parse(config)
	if err != nil {
		log.Fatal("Error configuring Bundlestore: ", err)
	}

	// Initialize Objects Based on Config Settings
	bag.InstallModule(mod)

	// Run Servers
	var servers servers
	err = bag.Extract(&servers)
	if err != nil {
		log.Fatal("Error injecting servers", err)
	}

	errCh := make(chan error)
	go func() {
		errCh <- servers.http.Serve()
	}()
	go func() {
		errCh <- servers.grpc.Serve()
	}()
	log.Fatal("Error serving:", <-errCh)
}
