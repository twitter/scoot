package local_test

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/runner/local"
)

func TestHttpOutput(t *testing.T) {
	tmp, _ := temp.NewTempDir("", "TestHttpOutput")
	defer os.RemoveAll(tmp.Dir)

	// Get an available port for the http server.
	ln, _ := net.Listen("tcp", "localhost:0")
	defer ln.Close()
	addr := ln.Addr().String()
	rootUri := "http://" + addr

	// Make HttpOutputCreator and then create and populate Output.
	creator, _ := local.NewHttpOutputCreator(tmp, rootUri)
	output, _ := creator.Create("stdout")
	output.Write([]byte("data"))

	// Start http server to forward requests to the HttpOutputCreator
	server := &http.Server{Addr: addr, Handler: creator}
	go server.Serve(ln)

	// Get and compare uri contents
	log.Print("getting: ", output.URI())
	resp, _ := http.Get(output.URI() + "&content=true")
	defer resp.Body.Close()
	data, _ := ioutil.ReadAll(resp.Body)
	if string(data) != "data" {
		t.Errorf("Invalid uri content: %v", string(data))
	}
}
