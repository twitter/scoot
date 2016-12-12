// Wrappers for receivers from the common/stats package and setting
// up an HTTP server with endpoints to make the stats data accessible.
package endpoints

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/scootdev/scoot/common/stats"
	"github.com/scootdev/scoot/os/temp"
)

// Returns an http handler at 'addr' which can retrieved with 'uri' if specified. Also serves static files from tmpDir if specified.
func NewTwitterServer(addr, uri string, stats stats.StatsReceiver, tmpDir *temp.TempDir) *TwitterServer {
	return &TwitterServer{
		Addr:            addr,
		Stats:           stats,
		TmpDir:          tmpDir,
		ResourceHandler: NewResourceHandler(uri, 2500*time.Millisecond),
	}
}

// A stats receiver that provides HTTP access for metric scraping with
// Twitter-style endpoints.
type TwitterServer struct {
	Addr            string
	Stats           stats.StatsReceiver
	TmpDir          *temp.TempDir
	ResourceHandler *ResourceHandler
}

func (s *TwitterServer) Serve() error {
	http.HandleFunc("/", helpHandler)
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/admin/metrics.json", s.statsHandler)
	if s.TmpDir != nil {
		http.Handle("/fs/", http.StripPrefix("/fs/", http.FileServer(http.Dir(s.TmpDir.Dir))))
	}
	log.Println("Serving http & stats on", s.Addr)
	return http.ListenAndServe(s.Addr, nil)
}

func helpHandler(w http.ResponseWriter, r *http.Request) {
	msg := "Common paths: '/health', '/admin/metrics.json', '/fs', '/{NAMESPACE}/stdout', '/{NAMESPACE}/stderr'"
	http.Error(w, msg, http.StatusNotImplemented)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "ok")
}

func (s *TwitterServer) statsHandler(w http.ResponseWriter, r *http.Request) {
	const contentTypeHdr = "Content-Type"
	const contentTypeVal = "application/json; charset=utf-8"
	w.Header().Set(contentTypeHdr, contentTypeVal)

	pretty := r.URL.Query().Get("pretty") == "true"
	str := s.Stats.Render(pretty)
	if _, err := io.Copy(w, bytes.NewBuffer(str)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

type StatScope string

func MakeStatsReceiver(scope StatScope) stats.StatsReceiver {
	s, _ := stats.NewCustomStatsReceiver(
		stats.NewFinagleStatsRegistry,
		15*time.Second)
	return s.Scope(string(scope))
}

const StdoutName = "stdout"
const StderrName = "stderr"

func NewResourceHandler(uri string, ajaxInterval time.Duration) *ResourceHandler {
	hostname, _ := os.Hostname()
	return &ResourceHandler{resources: make(map[string]map[string]string), hostname: hostname, uri: uri, ajaxInterval: ajaxInterval}
}

type ResourceHandler struct {
	// Defines: map[Namespace]map[ResourceName]ResourcePath
	resources    map[string]map[string]string
	hostname     string
	uri          string
	ajaxInterval time.Duration
	mutex        sync.Mutex
}

// Adds new http handlers for namespace,name,absPath and returns a unique associated uri.
// Registers two uris: '/resource/NAMESPACE/NAME' and '/resourcePID/NAMESPACE/NAME'
// Returns empty string if the base ResourceHandler.uri was not provided.
func (h *ResourceHandler) AddResource(namespace, name, absPath string) (uri string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if _, ok := h.resources[namespace]; !ok {
		h.resources[namespace] = make(map[string]string)
	}
	h.resources[namespace][name] = absPath
	httpPath := fmt.Sprintf("/resource/%s/%s", namespace, name)
	httpUniqPath := fmt.Sprintf("/resource_pid%d/%s/%s", os.Getpid(), namespace, name)
	http.Handle(httpPath, h)
	http.Handle(httpUniqPath, h)
	fileUri := fmt.Sprintf("file://%s%s", h.hostname, absPath)
	return fmt.Sprintf("%s%s?file=%s", h.uri, httpUniqPath, fileUri)
}

// Serves a minimal page that does ajax log tailing of the specified resource.
func (h *ResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	clientHtml :=
		`<html>
<script type="text/javascript">
  var prevLength = 0
  checkAtBottom = function() {
    //scrolling: http://stackoverflow.com/a/22394544
    var scrollTop = (document.documentElement && document.documentElement.scrollTop) || document.body.scrollTop;
    var scrollHeight = (document.documentElement && document.documentElement.scrollHeight) || document.body.scrollHeight;
    return (scrollTop + window.innerHeight) >= scrollHeight;
  }
  gotoBottom = function() {
    var scrollHeight = (document.documentElement && document.documentElement.scrollHeight) || document.body.scrollHeight;
    var scrollLeft = (document.documentElement && document.documentElement.scrollLeft) || document.body.scrollLeft;
    window.scrollTo(scrollLeft, scrollHeight);
  }
  sendRequest = function() {
    var xhr = new XMLHttpRequest();
    xhr.onreadystatechange = function () {
      var DONE=4, OK=200;
      if (xhr.readyState === DONE && xhr.status == OK) {
        var wasAtBottom = checkAtBottom()
        document.body.innerText += xhr.responseText.substring(prevLength);
        if (wasAtBottom)
          gotoBottom()
        prevLength = xhr.responseText.length
      }
    }
    //TODO: request range to get delta.
    xhr.open("GET", location.href + (location.search=="" ? "?" : "&") + "content=true");
    xhr.send();
  };
  setInterval(sendRequest, %d)
</script>
</html>
`
	paths := strings.Split(r.URL.Path, "/")
	namespace := paths[2]
	name := paths[3]
	resourcePath := h.resources[namespace][name]
	if resource, err := os.Open(resourcePath); err != nil {
		http.Error(w, "", http.StatusGone)
	} else if info, err := resource.Stat(); err != nil {
		http.Error(w, "", http.StatusInternalServerError)
	} else {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.URL.Query().Get("content") == "true" {
			http.ServeContent(w, r, "", info.ModTime(), resource)
		} else {
			fmt.Fprintf(w, clientHtml, h.ajaxInterval/time.Millisecond)
		}
	}
}
