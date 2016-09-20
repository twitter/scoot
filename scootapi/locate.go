package scootapi

import (
	"io/ioutil"
	"os"
	"path"
)

// Where is Cloud Scoot running?
// We store the answer (as host:port) in ~/.cloudscootaddr

// Get the path of the file containing the address for scootapi to use
func GetScootapiAddrPath() string {
	return path.Join(os.Getenv("HOME"), ".cloudscootaddr")
}

// Get the scootapi address (as host:port)
func GetScootapiAddr() string {
	data, _ := ioutil.ReadFile(GetScootapiAddrPath())
	return string(data)
}

// Set the scootapi address (as host:port)
func SetScootapiAddr(addr string) {
	ioutil.WriteFile(GetScootapiAddrPath(), []byte(addr), 0777)
}
