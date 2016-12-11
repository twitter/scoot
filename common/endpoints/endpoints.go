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
	hostname, _ := os.Hostname()
	return &TwitterServer{
		Addr:            addr,
		Stats:           stats,
		TmpDir:          tmpDir,
		ResourceHandler: &ResourceHandler{resources: make(map[string]map[string]string), hostname: hostname, uri: uri},
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

type ResourceHandler struct {
	// Defines: map[Namespace]map[ResourceName]ResourcePath
	resources map[string]map[string]string
	hostname  string
	uri       string
	mutex     sync.Mutex
}

// Adds a new http handler for namespace,name,path and returns the associated uri.
// Returns empty string if the base ResourceHandler.uri was not provided.
func (h *ResourceHandler) AddResource(namespace, name, path string) (uri string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if _, ok := h.resources[namespace]; !ok {
		h.resources[namespace] = make(map[string]string)
	}
	h.resources[namespace][name] = path
	httpAbsPath := fmt.Sprintf("/%s/%s", namespace, name)
	http.Handle(httpAbsPath, h)
	return fmt.Sprintf("%s%s", h.uri, httpAbsPath)
}

// Serves a minimal page that does ajax log tailing of the specified resource.
func (h *ResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	clientHtml :=
		`<html>
<title>%s</title>
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
    xhr.open("GET", location.href+"?content=true"); //TODO: request range
    xhr.send();
  };
  setInterval(sendRequest, 2500)
</script>
</html>
`
	paths := strings.Split(r.URL.Path, "/")
	namespace := paths[1]
	name := paths[2]
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
			fmt.Fprintf(w, clientHtml, h.hostname+resourcePath)
		}
	}
}
