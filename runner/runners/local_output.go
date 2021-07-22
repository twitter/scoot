package runners

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/twitter/scoot/runner"
	osexecer "github.com/twitter/scoot/runner/execer/os"
)

// local_output.go: output that's stored locally

type HttpOutputCreator interface {
	http.Handler
	runner.OutputCreator
	HttpPath() string
}

type localOutputCreator struct {
	tmp      string
	hostname string
	httpUri  string
	httpPath string
	pathMap  map[string]string
}

// Takes a tempdir to place new files and optionally an httpUri, ex: 'http://HOST:PORT/ENDPOINT/', to use instead of 'file://HOST/PATH'
func NewHttpOutputCreator(httpUri string) (HttpOutputCreator, error) {
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
	tmp, err := ioutil.TempDir("", "output")
	if err != nil {
		return nil, err
	}
	return &localOutputCreator{
		tmp: tmp, hostname: hostname,
		httpUri: httpUri, httpPath: httpPath,
		pathMap: make(map[string]string),
	}, nil
}

// Create a new Output that writes to local fs.
// Note: id should not have leading or trailing slashes.
func (s *localOutputCreator) Create(id string) (runner.Output, error) {
	if _, err := os.Stat(s.tmp); os.IsNotExist(err) {
		err = os.MkdirAll(s.tmp, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	f, err := ioutil.TempFile(s.tmp, id)
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
	clientHtml :=
		`<html>
		<script type="text/javascript">
			// This code makes use of ES6+ constructs, such as
			//   arrow functions: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Functions/Arrow_functions
			//   async await: https://developer.mozilla.org/en-US/docs/Learn/JavaScript/Asynchronous/Async_await
			//   promise: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise
			//   typed arrays: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Typed_arrays
			let resourceId = "";
			const oneHour = 60 * 60 * 1000;
			const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
			const checkAtBottom = () => {
				//scrolling: http://stackoverflow.com/a/22394544
				let scrollTop =
					(document.documentElement && document.documentElement.scrollTop) ||
					document.body.scrollTop;
				let scrollHeight =
					(document.documentElement && document.documentElement.scrollHeight) ||
					document.body.scrollHeight;
				return scrollTop + window.innerHeight >= scrollHeight;
			};
	
			const gotoBottom = () => {
				let scrollHeight =
					(document.documentElement && document.documentElement.scrollHeight) ||
					document.body.scrollHeight;
				let scrollLeft =
					(document.documentElement && document.documentElement.scrollLeft) ||
					document.body.scrollLeft;
				window.scrollTo(scrollLeft, scrollHeight);
			};
	
			// parseLength parses the Content-Range http header for the size of the resource
			const parseLength = resp => {
				let contentRange = resp.headers.get("Content-Range");
				let idx = contentRange.lastIndexOf("/") + 1;
				return contentRange.slice(idx);
			};
	
			const copyBuffer = (oldBuffer, length) => {
				buffer = new Uint8Array(length);
				buffer.set(oldBuffer);
				return {
					buffer,
					length
				};
			};
	
			const writeBuffer = (mainBuffer, arrayBuffer, offset) => {
				let bytes = new Uint8Array(arrayBuffer);
				for (let i = 0; i < bytes.byteLength; i++) {
					mainBuffer[i + offset] = bytes[i];
				}
			};
	
			const updateText = buffer => {
				let wasAtBottom = checkAtBottom();
				let div = document.getElementById("output");
				div.innerText = new TextDecoder("utf-8").decode(buffer);
				if (wasAtBottom) {
					gotoBottom();
				}
			};
	
			const increaseTimeout = currTimeout => Math.trunc((currTimeout * 3) / 2);
			sendRequest = async () => {
				let url =
					location.href + (location.search == "" ? "?" : "&") + "content=true";
				let resp = await fetch(url, {
					method: "HEAD"
				});
				let length = Number.parseInt(resp.headers.get("Content-Length"));
				// buffer is an Uint8Array to preserve utf-8 encoding
				let buffer = new Uint8Array(length);
				let curr = 0;
				// 1280KB was chosen as it would be larger than most small files, and
				// for larger files, say around 20MB, would be able to be retrieved in
				// approximately 20 calls.
				// 1280KB == 1310720 bytes
				let offset = 1310720;
				let minTimeout = 50;
				let currTimeout = minTimeout;
				// 15 minutes
				let maxTimeout = 15 * 60 * 1000;
				// Read 1280KB chunks until the entire resource is consumed, and wait for updates
				// up to one hour after last modified date
				while (true) {
					let next = Math.min(curr + offset, length);
					// if curr == next then we have reached the end of our file
					// and need to wait for updates to be written
					if (curr == next) {
						let resp = await fetch(url, {
							method: "HEAD"
						});
						length = Number.parseInt(resp.headers.get("Content-Length"));
						// Date.parse and Date.now returns epoch time
						lastModified = Date.parse(resp.headers.get("Last-Modified"));
						if (Date.now() - lastModified > oneHour) {
							// log hasn't been update in over 1 hour
							// so stop fetching
							break;
						}
						currTimeout = increaseTimeout(currTimeout);
					} else {
						// Make an HTTP Range Request
						// https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests
						resp = await fetch(url, {
							headers: new Headers({
								Range: "bytes=" + curr + "-" + next
							})
						});
						if (200 <= resp.status && resp.status < 300) {
							let id = resp.headers.get("X-Resource-Id");
							let newLength = parseLength(resp);
							// if newLength != length that means the file is still being written to
							// so increase the time between retries
							if (newLength != length) {
								minTimeout = 5000;
								({ buffer, length } = copyBuffer(buffer, newLength));
							}
							writeBuffer(buffer, await resp.arrayBuffer(), curr);
							updateText(buffer);
							if (resourceId == "") {
								resourceId = id;
							}
							if (id != resourceId) {
								alert("Underlying resource changed! Quitting");
								break;
							}
							currTimeout = minTimeout;
							curr = next;
						} else {
							currTimeout = increaseTimeout(currTimeout);
						}
					}
					await sleep(Math.min(currTimeout, maxTimeout));
				}
			};
			sendRequest();
		</script>
		<body>
			<div id="output" style="white-space: pre-wrap"></div>
		</body>
	</html>	
`
	if strings.TrimSuffix(r.URL.Path, "/")+"/" == s.HttpPath() {
		http.StripPrefix(s.HttpPath(), http.FileServer(http.Dir(s.tmp))).ServeHTTP(w, r)
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
		w.Header().Set("X-Resource-Id", filepath)
		if r.URL.Query().Get("content") == "true" {
			w.Header().Set("Cache-Control", "no-store")
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

// URI returns a URI to this Output
func (o *localOutput) URI() string {
	return o.uri
}

// AsFile returns an absolute path to a file with this content
func (o *localOutput) AsFile() string {
	return o.absPath
}

// Write implements io.Writer
func (o *localOutput) Write(p []byte) (n int, err error) {
	return o.f.Write(p)
}

// Close implements io.Closer
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
