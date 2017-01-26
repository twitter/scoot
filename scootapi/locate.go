package scootapi

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
)

// Where is Cloud Scoot running?
// We store the answer (as host:port) in ~/.cloudscootaddr
// There are two lines: first is the sched thrift addr, second is the bundlestore http addr.

// Get the path of the file containing the address for scootapi to use
func GetScootapiAddrPath() string {
	return path.Join(os.Getenv("HOME"), ".cloudscootaddr")
}

// Get the scootapi address (as host:port)
func GetScootapiAddr() (sched string, api string) {
	data, _ := ioutil.ReadFile(GetScootapiAddrPath())
	addrs := strings.Split(string(data), "\n")
	return string(addrs[0]), string(addrs[1])
}

// Set the scootapi address (as host:port)
func SetScootapiAddr(sched string, api string) {
	ioutil.WriteFile(GetScootapiAddrPath(), []byte(sched+"\n"+api), 0777)
}
