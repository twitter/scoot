package endpoints

import (
	"github.com/scootdev/scoot/common/log"
	"net/http"
	"time"

	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/ice"
)

// Module returns a module that supports serving HTTP
// Important to also install a function that returns map[string]http.Handler
func Module() ice.Module {
	return module{}
}

type module struct{}

// Install installs functions for serving HTTP
func (m module) Install(b *ice.MagicBag) {
	b.PutMany(
		func(scope StatScope) stats.StatsReceiver {
			return MakeStatsReceiver(scope).Precision(time.Millisecond)
		},
		func() map[string]http.Handler {
			return map[string]http.Handler{}
		},
		NewTwitterServer,
	)

}

// Starts the Server based on the MagicBag and config schema provided
// this method blocks until the server completes running or an error occurs.
func RunServer(bag *ice.MagicBag, schema jsonconfig.Schema, config []byte) {
	// Parse Config
	log.Info("common/endpoints RunServer(), config is:", string(config))
	mod, err := schema.Parse(config)
	if err != nil {
		log.Crit("Error configuring Scoot API: ", err)
	}

	// Initialize Objects Based on Config Settings
	bag.InstallModule(mod)

	// Run Servers
	var server *TwitterServer
	err = bag.Extract(&server)
	if err != nil {
		log.Crit("Error injecting server", err)
	}
	if err = server.Serve(); err != nil {
		log.Crit(err.Error())
	}
}
