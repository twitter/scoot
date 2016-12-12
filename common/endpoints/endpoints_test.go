package endpoints_test

import (
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/scootdev/scoot/common/endpoints"
)

func Test_Resource(t *testing.T) {
	// Start http server to forward requests to ResourceHandler
	ln, _ := net.Listen("tcp", "localhost:0")
	defer ln.Close()
	addr := ln.Addr().String()
	rootUri := "http://" + addr
	handler := endpoints.NewResourceHandler(rootUri, 0)
	server := &http.Server{Addr: addr, Handler: handler}
	go server.Serve(ln)

	// Make a resource and populate with initial content.
	file, _ := ioutil.TempFile("", "test_resource")
	defer file.Close()
	defer os.Remove(file.Name())
	file.Write([]byte("foo"))

	// Register that resource
	uri := handler.AddResource("ns", "stdout", file.Name())

	// Get and compare uri contents
	resp, _ := http.Get(uri + "&content=true")
	defer resp.Body.Close()
	data, _ := ioutil.ReadAll(resp.Body)
	if string(data) != "foo" {
		t.Errorf("Invalid uri content: %v", data)
	}
}
