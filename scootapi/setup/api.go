package setup

import (
	"log"
	"strings"
)

// ApiStrategy will startup with a bundlestore (or setup a connection to one)
type ApiStrategy interface {

	// Startup starts up a Scheduler, returing the address of the server or an error
	Startup() (string, error)
}

type ApiConfig struct {
	StoreAddr string
}

// LocalApiStrategy starts up a local apiserver
type LocalApiStrategy struct {
	apiCfg  *ApiConfig
	builder Builder
	cmds    *Cmds
}

// Create a new Local ApiServer that will serve the bundlestore api, using builder and cmds to start
func NewLocalApiStrategy(apiCfg *ApiConfig, builder Builder, cmds *Cmds) *LocalApiStrategy {
	return &LocalApiStrategy{
		apiCfg:  apiCfg,
		builder: builder,
		cmds:    cmds,
	}
}

func (s *LocalApiStrategy) Startup() (string, error) {
	log.Println("Starting up a Local ApiServer")

	bin, err := s.builder.ApiServer()
	if err != nil {
		return "", err
	}

	if err := s.cmds.Start(bin, "-http_addr", s.apiCfg.StoreAddr); err != nil {
		return "", err
	}

	if err := WaitForPort(strings.Split(s.apiCfg.StoreAddr, ":")[1]); err != nil {
		return "", err
	}

	return s.apiCfg.StoreAddr, nil
}

// Create an ApiServer Strategy with a local apiserver.
func NewLocal(apiCfg *ApiConfig, builder Builder, cmds *Cmds) *LocalApiStrategy {
	return NewLocalApiStrategy(apiCfg, builder, cmds)
}
