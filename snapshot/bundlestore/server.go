package bundlestore

import (
	"net/http"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/cas"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/snapshot/store"
)

// Represents a Bundlestore Server that serves HTTP and gRPC Bazel CAS APIs
// over an underlying Store with shared configuration and stats.
// StoreConfig is in part as a simplified solution to 2 problems golang-related
// problems we have with our current architecture:
// - values of Store and StatsReceiver interfaces don't pass down cleanly
// - encapsulating store, config and stats here prevented from being reused in cas
// because of cyclic dependency rules
type Server struct {
	storeConfig *store.StoreConfig
	httpServer  *httpServer
	casServer   bazel.GRPCServer
	concurrent  chan struct{}
}

// Make a new server that delegates to an underlying store.
// TTL may be nil, in which case defaults are applied downstream.
// TTL duration may be overriden by request headers, but we always pass this TTLKey to the store.
func MakeServer(s store.Store, ttl *store.TTLConfig, stat stats.StatsReceiver, l bazel.GRPCListener) *Server {
	scopedStat := stat.Scope("bundlestoreServer")
	go stats.StartUptimeReporting(scopedStat, stats.BundlestoreUptime_ms, stats.BundlestoreServerStartedGauge, stats.DefaultStartupGaugeSpikeLen)
	cfg := &store.StoreConfig{Store: s, TTLCfg: ttl, Stat: scopedStat}
	log.Infof("Starting new bundlestore.Server with root: %s", s.Root())

	return &Server{
		storeConfig: cfg,
		httpServer:  MakeHTTPServer(cfg),
		casServer:   cas.MakeCASServer(l, cfg, stat),
		concurrent:  make(chan struct{}, MaxConnections),
	}
}

// Implements http.Handler interface
func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if s.concurrent != nil {
		s.concurrent <- struct{}{}
		defer func() { <-s.concurrent }()
	}

	s.storeConfig.Stat.Counter(stats.BundlestoreRequestCounter).Inc(1)
	switch req.Method {
	case "POST":
		s.httpServer.HandleUpload(w, req)
	case "HEAD":
		fallthrough
	case "GET":
		s.httpServer.HandleDownload(w, req)
	default:
		log.Infof("Request err: %v --> StatusMethodNotAllowed (from %v)", req.Method, req.RemoteAddr)
		http.Error(w, "only support POST and GET", http.StatusMethodNotAllowed)
		return
	}
	s.storeConfig.Stat.Counter(stats.BundlestoreRequestOkCounter).Inc(1)
}
