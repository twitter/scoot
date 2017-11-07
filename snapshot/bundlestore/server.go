package bundlestore

import (
	"fmt"
	"net"
	"net/http"
	"regexp"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/stats"
)

type CommonStuff struct {
	store      Store
	ttlCfg     *TTLConfig
	stat       stats.StatsReceiver
}

type Server struct {
	//store      Store
	//ttlCfg     *TTLConfig
	//stat       stats.StatsReceiver
	stuff      *CommonStuff
	httpServer *httpServer
	casServer  *casServer
}

// Make a new server that delegates to an underlying store.
// TTL may be nil, in which case defaults are applied downstream.
// TTL duration may be overriden by request headers, but we always pass this TTLKey to the store.
func MakeServer(s Store, ttl *TTLConfig, stat stats.StatsReceiver, l net.Listener) *Server {
	scopedStat := stat.Scope("bundlestoreServer")
	go stats.StartUptimeReporting(scopedStat, stats.BundlestoreUptime_ms, stats.BundlestoreServerStartedGauge, stats.DefaultStartupGaugeSpikeLen)

	stuffz := CommonStuff{s, ttl, stat}

	return &Server{
		//store:      s,
		//ttlCfg:     ttl,
		//stat:       scopedStat,
		stuff:      &stuffz,
		//httpServer: MakeHTTPServer(s, ttl, stat),
		httpServer: MakeHTTPServer(&stuffz),
		//casServer:  NewCASServer(l, s, ttl, stat),
		casServer:  NewCASServer(l, &stuffz),
	}
}

// Implements http.Handler interface
func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	//s.stat.Counter(stats.BundlestoreRequestCounter).Inc(1)
	s.stuff.stat.Counter(stats.BundlestoreRequestCounter).Inc(1)
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
	//s.stat.Counter(stats.BundlestoreRequestOkCounter).Inc(1)
	s.stuff.stat.Counter(stats.BundlestoreRequestOkCounter).Inc(1)
}

func CheckBundleName(name string) error {
	bundleRE := "^bs-[a-z0-9]{40}.bundle"
	if ok, _ := regexp.MatchString(bundleRE, name); ok {
		return nil
	}
	return fmt.Errorf("Error with bundleName, expected %q, got: %s", bundleRE, name)
}
