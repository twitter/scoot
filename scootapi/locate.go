package scootapi

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

// Where is Cloud Scoot running?
// We store the answer (as host:port\nhost:port) in ~/.cloudscootaddr
// There are two lines: first is the sched thrift addr, second is the bundlestore http addr.
// The user may set SCOOT_ID=<ArbitraryId> to refer to a Scoot Cloud address other than the default.
//  This is useful, for example, in dev testing of multiple *remote* Cloud Scoot instances.
//
// TODO: this will eventually store only the thrift addr and http addr
//       for a single instance of apiserver, though several may be running.
// TODO: can we get rid of this and exclusively rely on a Fetcher to find instances?

// Get the path of the file containing the address for scootapi to use
func GetScootapiAddrPath() string {
	optionalId := os.Getenv("SCOOT_ID") // Used to connect to a different set of scoot processes.
	return path.Join(os.Getenv("HOME"), ".cloudscootaddr"+optionalId)
}

// Get the scootapi address (as host:port)
func GetScootapiAddr() (sched string, api string, err error) {
	data, err := ioutil.ReadFile(GetScootapiAddrPath())
	if err != nil {
		if os.IsNotExist(err) {
			return "", "", nil
		}
		return "", "", err
	}
	addrs := strings.Split(string(data), "\n")
	if len(addrs) != 2 {
		return "", "", errors.New("Expected both sched and api addrs, got: " + string(data))
	}
	return string(addrs[0]), string(addrs[1]), nil
}

// Set the scootapi address (as host:port)
func SetScootapiAddr(sched string, api string) {
	ioutil.WriteFile(GetScootapiAddrPath(), []byte(sched+"\n"+api), 0777)
}

// Create a Bundlestore URI from an addr
func APIAddrToBundlestoreURI(addr string) string {
	return "http://" + addr + "/bundle/"
}

// BundlestoreResolver resolves a URI to Bundlestore
type BundlestoreResolver struct{}

// NewBundlestoreResolver creates a new BundlestoreResolver
func NewBundlestoreResolver() *BundlestoreResolver {
	return &BundlestoreResolver{}
}

// Resolve resolves a URI to Bundlestore
func (r *BundlestoreResolver) Resolve() (string, error) {
	_, s, err := GetScootapiAddr()
	if s == "" || err != nil {
		return "", err
	}
	return APIAddrToBundlestoreURI(s), nil
}
