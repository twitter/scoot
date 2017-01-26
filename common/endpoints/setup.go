package endpoints

import (
	"log"
	"time"

	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/config/jsonconfig"
	"github.com/scootdev/scoot/ice"
)

func Module() ice.Module {
	return module{}
}

type module struct{}

func (m module) Install(b *ice.MagicBag) {
	b.PutMany(
		func(scope StatScope) stats.StatsReceiver {
			return MakeStatsReceiver(scope).Precision(time.Millisecond)
		},
		NewTwitterServer,
	)

}

// Starts the Server based on the MagicBag and config schema provided
// this method blocks until the server completes running or an error occurs.
func RunServer(bag *ice.MagicBag, schema jsonconfig.Schema, config []byte) {
	// Parse Config
	mod, err := schema.Parse(config)
	if err != nil {
		log.Fatal("Error configuring Scoot API: ", err)
	}

	// Initialize Objects Based on Config Settings
	bag.InstallModule(mod)

	// Run Servers
	var server *TwitterServer
	err = bag.Extract(&server)
	if err != nil {
		log.Fatal("Error injecting server", err)
	}

	log.Fatal(server.Serve())
}
