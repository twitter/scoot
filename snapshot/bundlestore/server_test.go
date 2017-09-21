package bundlestore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/twitter/scoot/common/stats"
)

type FakeStore struct {
	files map[string][]byte
	ttl   *TTLValue
}

func (f *FakeStore) Exists(name string) (bool, error) {
	if _, ok := f.files[name]; !ok {
		return false, nil
	}
	return true, nil
}

func (f *FakeStore) OpenForRead(name string) (io.ReadCloser, error) {
	if ok, _ := f.Exists(name); !ok {
		return nil, errors.New("Doesn't exist :" + name)
	}
	return ioutil.NopCloser(bytes.NewBuffer(f.files[name])), nil
}

func (f *FakeStore) Root() string { return "" }

func (f *FakeStore) Write(name string, data io.Reader, ttl *TTLValue) error {
	if (f.ttl == nil) != (ttl == nil) || (ttl != nil && (f.ttl.TTLKey != ttl.TTLKey || f.ttl.TTL.Sub(ttl.TTL) != 0)) {
		return fmt.Errorf("TTL mismatch: expected: %v, got: %v", f.ttl, ttl)
	} else if data, err := ioutil.ReadAll(data); err != nil {
		return err
	} else {
		f.files[name] = data
		return nil
	}
}

//TODO: an end-end test that uses a real store and real bundles.

func TestServer(t *testing.T) {
	// Construct server with a fake store and random port address.
	now := time.Time{}.Add(time.Minute)
	store := &FakeStore{files: map[string][]byte{}, ttl: nil}
	DefaultTTL = 0

	listener, _ := net.Listen("tcp", "localhost:0")
	defer listener.Close()
	addr := listener.Addr().String()

	statsRegistry := stats.NewFinagleStatsRegistry()
	statsReceiver, _ := stats.NewCustomStatsReceiver(func() stats.StatsRegistry { return statsRegistry }, 0)
	stats.StatReportIntvl = 20 * time.Millisecond
	server := MakeServer(store, nil, statsReceiver)
	mux := http.NewServeMux()
	mux.Handle("/bundle/", server)
	go func() {
		http.Serve(listener, mux)
	}()

	rootUri := "http://" + addr + "/bundle/"
	client := &http.Client{Timeout: 1 * time.Second}

	// Try to write data.
	bundle1ID := "bs-0000000000000000000000000000000000000001.bundle"
	if resp, err := client.Post(rootUri+bundle1ID, "text/plain", bytes.NewBuffer([]byte("baz_data"))); err != nil {
		t.Fatal(err.Error())
	} else {
		if resp.StatusCode != http.StatusOK {
			body, err := ioutil.ReadAll(resp.Body)
			t.Fatal(resp.Status, string(body), err)
		}
		resp.Body.Close()
	}

	if !reflect.DeepEqual(store.files[bundle1ID], []byte("baz_data")) {
		t.Fatalf("Failed to post data")
	}

	time.Sleep(2*stats.StatReportIntvl + (10 * time.Millisecond))
	if !stats.StatsOk("", statsRegistry, t,
		map[string]stats.Rule{
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreServerStartedGauge):    {Checker: stats.Int64EqTest, Value: 1},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreUploadCounter):         {Checker: stats.Int64EqTest, Value: 1},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreUploadOkCounter):       {Checker: stats.Int64EqTest, Value: 1},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreUploadExistingCounter): {Checker: stats.Int64EqTest, Value: nil},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreUploadErrCounter):      {Checker: stats.Int64EqTest, Value: nil},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadCounter):       {Checker: stats.Int64EqTest, Value: nil},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadOkCounter):     {Checker: stats.Int64EqTest, Value: nil},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadErrCounter):    {Checker: stats.Int64EqTest, Value: nil},
		}) {
		t.Fatal("stats check did not pass.")
	}

	// Try to write data again - should trigger upload existing
	if resp, err := client.Post(rootUri+bundle1ID, "text/plain", bytes.NewBuffer([]byte("baz_data"))); err != nil {
		t.Fatalf(err.Error())
	} else {
		resp.Body.Close()
	}

	if !stats.StatsOk("", statsRegistry, t,
		map[string]stats.Rule{
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreUploadCounter):         {Checker: stats.Int64EqTest, Value: 2},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreUploadOkCounter):       {Checker: stats.Int64EqTest, Value: 1},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreUploadExistingCounter): {Checker: stats.Int64EqTest, Value: 1},
		}) {
		t.Fatal("stats check did not pass.")
	}

	// Try to read data.
	if resp, err := client.Get(rootUri + bundle1ID); err != nil {
		t.Fatalf(err.Error())
	} else {
		defer resp.Body.Close()
		if data, err := ioutil.ReadAll(resp.Body); err != nil {
			t.Fatalf(err.Error())
		} else if !reflect.DeepEqual(data, []byte("baz_data")) {
			t.Fatalf("Failed to get data.")
		}
	}

	if !stats.StatsOk("", statsRegistry, t,
		map[string]stats.Rule{
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadCounter):   {Checker: stats.Int64EqTest, Value: 1},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadOkCounter): {Checker: stats.Int64EqTest, Value: 1},
		}) {
		t.Fatal("stats check did not pass.")
	}

	// Try to read non-existent data, expect NotFound.
	if resp, err := client.Get(rootUri + "bs-0000000000000000000000000000000000000000.bundle"); err != nil {
		t.Fatalf(err.Error())
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("Expected NotFound when getting non-existent data.")
		}
	}

	if !stats.StatsOk("", statsRegistry, t,
		map[string]stats.Rule{
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadCounter):    {Checker: stats.Int64EqTest, Value: 2},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadOkCounter):  {Checker: stats.Int64EqTest, Value: 1},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadErrCounter): {Checker: stats.Int64EqTest, Value: 1},
		}) {
		t.Fatal("stats check did not pass.")
	}

	//
	// Repeat with HttpStore as the client.
	//

	// Try to write data.
	// We send a new TTL and reconfigure the server to transform the original ttl key to a new one.
	store.ttl = &TTLValue{now.Add(time.Hour), "NEW_TTL"}
	server.ttlCfg = &TTLConfig{0, "NEW_TTL"}
	clientTTL := &TTLValue{now.Add(time.Hour), DefaultTTLKey}
	hs := MakeHTTPStore(rootUri)
	bundle2ID := "bs-0000000000000000000000000000000000000002.bundle"
	if err := hs.Write(bundle2ID, bytes.NewBuffer([]byte("bar_data")), clientTTL); err != nil {
		t.Fatalf(err.Error())
	}

	if !stats.StatsOk("", statsRegistry, t,
		map[string]stats.Rule{
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreUploadCounter):         {Checker: stats.Int64EqTest, Value: 3},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreUploadOkCounter):       {Checker: stats.Int64EqTest, Value: 2},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreUploadExistingCounter): {Checker: stats.Int64EqTest, Value: 1},
		}) {
		t.Fatal("stats check did not pass.")
	}

	// Check if the write succeeded.
	if ok, err := hs.Exists(bundle2ID); err != nil {
		t.Fatalf(err.Error())
	} else if !ok {
		t.Fatalf("Expected data to exist.")
	}

	if !stats.StatsOk("", statsRegistry, t,
		map[string]stats.Rule{
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadCounter):    {Checker: stats.Int64EqTest, Value: 3},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadOkCounter):  {Checker: stats.Int64EqTest, Value: 2},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadErrCounter): {Checker: stats.Int64EqTest, Value: 1},
		}) {
		t.Fatal("stats check did not pass.")
	}

	// Try to read data.
	if reader, err := hs.OpenForRead(bundle2ID); err != nil {
		t.Fatalf(err.Error())
	} else if data, err := ioutil.ReadAll(reader); err != nil {
		t.Fatalf(err.Error())
	} else if !reflect.DeepEqual(data, []byte("bar_data")) {
		t.Fatalf("Failed to get matching data: " + string(data))
	}

	if !stats.StatsOk("", statsRegistry, t,
		map[string]stats.Rule{
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadCounter):    {Checker: stats.Int64EqTest, Value: 4},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadOkCounter):  {Checker: stats.Int64EqTest, Value: 3},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadErrCounter): {Checker: stats.Int64EqTest, Value: 1},
		}) {
		t.Fatal("stats check did not pass.")
	}

	// Check for non-existent data.
	if ok, err := hs.Exists("bs-0000000000000000000000000000000000000000.bundle"); err != nil {
		t.Fatalf(err.Error())
	} else if ok {
		t.Fatalf("Expected data to not exist.")
	}

	if !stats.StatsOk("", statsRegistry, t,
		map[string]stats.Rule{
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadCounter):    {Checker: stats.Int64EqTest, Value: 5},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadOkCounter):  {Checker: stats.Int64EqTest, Value: 3},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadErrCounter): {Checker: stats.Int64EqTest, Value: 2},
		}) {
		t.Fatal("stats check did not pass.")
	}

	// Check for invalid name error
	if _, err := hs.Exists("foo"); err == nil {
		t.Fatalf("Expected invalid input err.")
	}

	// check the stats
	if !stats.StatsOk("", statsRegistry, t,
		map[string]stats.Rule{
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreUploadCounter):         {Checker: stats.Int64EqTest, Value: 3},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreUploadOkCounter):       {Checker: stats.Int64EqTest, Value: 2},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreUploadExistingCounter): {Checker: stats.Int64EqTest, Value: 1},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreUploadErrCounter):      {Checker: stats.Int64EqTest, Value: nil},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadCounter):       {Checker: stats.Int64EqTest, Value: 6},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadOkCounter):     {Checker: stats.Int64EqTest, Value: 3},
			fmt.Sprintf("bundlestoreServer/%s", stats.BundlestoreDownloadErrCounter):    {Checker: stats.Int64EqTest, Value: 3},
		}) {
		t.Fatal("stats check did not pass.")
	}

}

type fakeServer struct {
	counter int
	code    []int
	times   []time.Time
}

func (s *fakeServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(s.code[s.counter])
	s.times = append(s.times, time.Now())
	s.counter++
}

func TestRetry(t *testing.T) {
	listener, _ := net.Listen("tcp", "localhost:0")
	defer listener.Close()
	addr := listener.Addr().String()

	server := &fakeServer{}
	mux := http.NewServeMux()
	mux.Handle("/bundle/", server)
	go func() {
		http.Serve(listener, mux)
	}()
	rootUri := "http://" + addr + "/bundle/"

	// Try 3 times then fail
	now := time.Now()
	server.times = []time.Time{}
	server.code = []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable}
	client := MakePesterClient()
	client.Backoff = func(_ int) time.Duration { return 500 * time.Millisecond }
	client.MaxRetries = 3
	hs := MakeCustomHTTPStore(rootUri, client)

	if _, err := hs.OpenForRead(""); err == nil {
		t.Fatalf("Expected err, got nil")
	}
	if server.counter != 3 {
		t.Fatalf("Expected 3 tries, got: %d", server.counter)
	}
	if server.times[2].Sub(now) > 1500*time.Millisecond ||
		server.times[2].Sub(now) < 1000*time.Millisecond ||
		server.times[1].Sub(now) < 500*time.Millisecond ||
		server.times[0].Sub(now) > 500*time.Millisecond {
		t.Fatalf("Expected 3 tries 500ms apart, got: %v", server.times)
	}

	// Try twice then succeed on the third time.
	server.counter = 0
	server.code = []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusOK}
	client.MaxRetries = 10
	if _, err := hs.OpenForRead("foo"); err != nil {
		t.Fatalf("Expected success, got: %v", err)
	}
	if server.counter != 3 {
		t.Fatalf("Expected 3 tries, got: %d", server.counter)
	}
}
