package setup

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"

	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/scootapi"
)

const DefaultApiServerCount int = 3
const DefaultApiServerLogLevel log.Level = log.InfoLevel

// ApiStrategy will startup with a bundlestore (or setup a connection to one)
type ApiStrategy interface {

	// Startup starts up an ApiServer, returing the address of the server or an error
	Startup() ([]string, error)
}

// We set the number of apiserver instances to start.
// A default value will be assigned if unitialized.
// We also set a logLevel, which determines the minimum level
// to display in apiserver logs.
type ApiConfig struct {
	Count    int
	LogLevel log.Level
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

func (s *LocalApiStrategy) Startup() ([]string, error) {
	log.Info("Starting up a Local ApiServer")
	if s.apiCfg.Count < 0 {
		return nil, fmt.Errorf("ApiServer count must be >0 (or zero for default #), was: %d", s.apiCfg.Count)
	} else if s.apiCfg.Count == 0 {
		s.apiCfg.Count = DefaultApiServerCount
	}

	// A log level of 0 corresponds to Panic. Default behavior shouldn't be to surpress all log output
	// besides log.Panic, so we set a default of Info.
	if s.apiCfg.LogLevel <= 0 {
		s.apiCfg.LogLevel = DefaultApiServerLogLevel
	}

	tmp, err := temp.TempDirDefault()
	if err != nil {
		return nil, err
	}
	bundlestoreStoreDir, err := tmp.FixedDir("common-bundles")
	if err != nil {
		return nil, err
	}

	bin, err := s.builder.ApiServer()
	if err != nil {
		return nil, err
	}

	addrs := []string{}
	for i := 0; i < s.apiCfg.Count; i++ {
		port := scootapi.ApiBundlestorePorts + i
		httpAddr := fmt.Sprintf("localhost:%d", port)
		cmd := s.cmds.Command(bin, "-http_addr", httpAddr, "-log_level", s.apiCfg.LogLevel.String())
		cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%s", scootapi.BundlestoreEnvVar, bundlestoreStoreDir.Dir))
		if err := s.cmds.StartCmd(cmd); err != nil {
			return nil, err
		}
		if err := WaitForPort(port); err != nil {
			return nil, err
		}
		addrs = append(addrs, httpAddr)
	}

	return addrs, nil
}

// Create an ApiServer Strategy with a local apiserver.
func NewLocal(apiCfg *ApiConfig, builder Builder, cmds *Cmds) *LocalApiStrategy {
	if apiCfg == nil {
		apiCfg = &ApiConfig{}
	}
	return NewLocalApiStrategy(apiCfg, builder, cmds)
}
