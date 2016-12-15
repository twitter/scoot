package local

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner"
	osexecer "github.com/scootdev/scoot/runner/execer/os"
)

type HttpOutputCreator interface {
	http.Handler
	runner.OutputCreator
	HttpPath() string
}

type localOutputCreator struct {
	tmp      *temp.TempDir
	hostname string
	httpUri  string
	httpPath string
	pathMap  map[string]string
	mutex    sync.Mutex
}

// Create a new OutputCreator
// Takes a tempdir to place new files and optionally a httpUri to use instead of 'file://HOSTNAME/'
func NewHttpOutputCreator(tmp *temp.TempDir, httpUri string) (HttpOutputCreator, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	httpPath := ""
	if httpUri != "" {
		u, err := url.Parse(httpUri)
		if err != nil {
			return nil, err
		}
		httpPath = strings.TrimSuffix(u.Path, "/") + "/"
	}
	return &localOutputCreator{
		tmp: tmp, hostname: hostname,
		httpUri: httpUri, httpPath: httpPath,
		pathMap: make(map[string]string),
	}, nil
}

func NewOutputCreator(tmp *temp.TempDir) (HttpOutputCreator, error) {
	return NewHttpOutputCreator(tmp, "")
}

// Create a new Output that writes to local fs.
// Note: id should not have leading or trailing slashes.
func (s *localOutputCreator) Create(id string) (runner.Output, error) {
	f, err := s.tmp.TempFile(id)
	if err != nil {
		return nil, err
	}
	absPath, err := filepath.Abs(f.Name())
	if err != nil {
		return nil, err
	}
	// We don't need a / between hostname and path because absolute paths start with /
	uri := fmt.Sprintf("file://%s%s", s.hostname, absPath)
	if s.httpUri != "" {
		uri = fmt.Sprintf("%s/%s?file=%s", s.httpUri, id, uri)
		s.pathMap[strings.Trim(id, "/")] = absPath
		s.pathMap[filepath.Base(absPath)] = absPath
	}
	return &localOutput{f: f, absPath: absPath, uri: uri}, nil
}

// Serves a minimal page that does ajax log tailing of the specified path
// When '?content=true' is specified, this serves the content directly without ajax.
// Does not check the request path, either it finds the local file or 404s.
func (s *localOutputCreator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

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
  sendRequest()
  setInterval(sendRequest, 2000)
</script>
</html>
`
	if strings.TrimSuffix(r.URL.Path, "/")+"/" == s.HttpPath() {
		http.StripPrefix(s.HttpPath(), http.FileServer(http.Dir(s.tmp.Dir))).ServeHTTP(w, r)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, s.HttpPath())
	filepath, ok := s.pathMap[path]
	if !ok {
		http.Error(w, "Unrecognized path", http.StatusNotFound)
	} else if resource, err := os.Open(filepath); err != nil {
		http.Error(w, "", http.StatusGone)
	} else if info, err := resource.Stat(); err != nil {
		http.Error(w, "", http.StatusInternalServerError)
	} else {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.URL.Query().Get("content") == "true" {
			http.ServeContent(w, r, "", info.ModTime(), resource)
		} else {
			fmt.Fprintf(w, clientHtml)
		}
	}
}

func (s *localOutputCreator) HttpPath() string {
	return s.httpPath
}

type localOutput struct {
	f       *os.File
	absPath string
	uri     string
}

func (o *localOutput) URI() string {
	return o.uri
}

func (o *localOutput) AsFile() string {
	return o.absPath
}

func (o *localOutput) Write(p []byte) (n int, err error) {
	return o.f.Write(p)
}

func (o *localOutput) Close() error {
	return o.f.Close()
}

// Return an underlying Writer. Why? Because some methods type assert to
// a more specific type and are more clever (e.g., if it's an *os.File, hook it up
// directly to a new process's stdout/stderr.)
// We care about this cleverness, so Output both is-a and has-a Writer
func (o *localOutput) WriterDelegate() io.Writer {
	return o.f
}

var _ osexecer.WriterDelegater = (*localOutput)(nil)
