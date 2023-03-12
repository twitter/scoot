package starter

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/proxy"

	"github.com/wisechengyi/scoot/cloud/cluster"
	"github.com/wisechengyi/scoot/common"
	"github.com/wisechengyi/scoot/common/dialer"
	"github.com/wisechengyi/scoot/runner"
	"github.com/wisechengyi/scoot/runner/runners"
	"github.com/wisechengyi/scoot/saga"
	"github.com/wisechengyi/scoot/saga/sagalogs"
	"github.com/wisechengyi/scoot/scheduler/scheduler/config"
	"github.com/wisechengyi/scoot/scheduler/server"
	"github.com/wisechengyi/scoot/worker/client"
)

const DEFAULT_SOCKS_PORT = "50001"
const DEFAULT_SOCKS_ADDR = "localhost:" + DEFAULT_SOCKS_PORT

func GetWorkerRunnerServiceFn(workers client.WorkersClientJSONConfig, thriftTransportFactory thrift.TTransportFactory, binaryProtocolFactory thrift.TProtocolFactory) (func(cluster.Node) runner.Service, error) {
	var rf func(cluster.Node) runner.Service
	var err error
	if workers.Type == "socks" {
		rf, err = GetSocksWorker()
	} else {
		rf, err = client.NewWorkerThriftClient(thriftTransportFactory, binaryProtocolFactory, common.DefaultClientTimeout, workers)
	}
	if err != nil {
		return nil, fmt.Errorf("error creating worker thrift client (runner.Service). %s", err)
	}

	return rf, nil
}

func GetSocksWorker() (server.RunnerFactory, error) {
	transportFactory, err := NewTTransportFactory()
	if err != nil {
		return nil, err
	}
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	wf := func(node cluster.Node) runner.Service {
		di := dialer.NewSimpleDialer(transportFactory, protocolFactory, common.DefaultClientTimeout)
		cl, _ := client.NewSimpleClient(di, string(node.Id()))
		return runners.NewPollingService(cl, cl, 250*time.Millisecond)
	}

	return wf, nil
}

func GetProxyAddr() string {
	port := os.Getenv("SOCKS_PORT")
	if port != "" {
		return "localhost:" + port
	}
	return ""
}

func GetProxyDialer(proxyAddr string) (proxy.Dialer, error) {
	return proxy.SOCKS5("tcp", proxyAddr, nil, proxy.Direct)
}

func NewTTransportFactory() (thrift.TTransportFactory, error) {
	addr := GetProxyAddr()
	if addr == "" {
		addr = DEFAULT_SOCKS_ADDR
	}
	dialer, err := GetProxyDialer(addr)
	if err != nil {
		return nil, err
	}
	return &socksTransportFactory{dialer}, nil
}

type socksTransportFactory struct {
	dialer proxy.Dialer
}

func (f *socksTransportFactory) GetTransport(trans thrift.TTransport) thrift.TTransport {
	// If this is a SOCKS-able connection, then return a TTransport that uses SOCKS.
	// If it's not, return trans unmodified.
	tsock, ok := trans.(*thrift.TSocket)
	if !ok {
		// Don't know how to wrap anything other than a TSocket
		return trans
	}
	if tsock.IsOpen() {
		// Transport Factory should be called before Open
		return trans
	}
	addr, ok := tsock.Addr().(*net.TCPAddr)
	if !ok {
		return trans
	}
	ip := addr.IP.To4()
	if ip == nil {
		return trans
	}

	// An Address is Socks-able if it starts with 10, because Twitter's production network lives in
	// 10/8 in IPv4 space.
	if ip[0] != 10 {
		return trans
	}

	if f.dialer == nil {
		// We want to use SOCKS, but can't, because we have no dialer
		log.Info("transport will not use SOCKS proxy because SOCKS_PORT is unset", addr.String())
		return trans
	}

	return &socksSocket{
		addr:    addr.String(),
		timeout: 0, // thrift.TSocket gives us no way to extract the timeout
		dialer:  f.dialer,
	}
}

// socksSocket creates a socket via the proxy dialer.
// (Ideally, there would be a form of TSocket that took a Dialer instead of a net.Conn)
type socksSocket struct {
	*thrift.TSocket
	addr    string
	timeout time.Duration
	dialer  proxy.Dialer
}

func (s *socksSocket) Open() error {
	if s.IsOpen() {
		return s.Open()
	}
	conn, err := s.dialer.Dial("tcp", s.addr)
	if err != nil {
		log.Infof("Failed connecting to %s via proxy, trying directly\n", s.addr)
		conn, err = net.DialTimeout("tcp", s.addr, time.Duration(10*time.Second))
		if err != nil {
			return err
		}
	}

	s.TSocket = thrift.NewTSocketFromConnTimeout(conn, 0)
	return nil
}

func StartCluster(clusterJSON config.ClusterJSONConfig) (chan []cluster.NodeUpdate, error) {
	var uc chan []cluster.NodeUpdate
	var err error
	if clusterJSON.Type == "inMemory" {
		cmc := &config.ClusterMemoryConfig{
			Count: clusterJSON.Count,
		}
		uc, err = cmc.Create()
	} else {
		clc := &config.ClusterLocalConfig{}
		uc, err = clc.Create()
	}
	if err != nil {
		return nil, fmt.Errorf("error creating cluster.  Scheduler not started. %s", err)
	}

	return uc, nil
}

// MakeSagaLog - TODO remove saga or refactor it so this function can be moved into saga or sagalog
// the current organization leads to cyclic dependency when moving to saga or sagalogs
func MakeSagaLog(config config.SagaLogJSONConfig) (saga.SagaLog, error) {
	if config.Type == "memory" {
		return sagalogs.MakeInMemorySagaLog(time.Duration(config.ExpirationSec)*time.Second, time.Duration(config.GCIntervalSec)*time.Second), nil
	}
	if config.Type == "file" {
		return sagalogs.MakeFileSagaLog(config.Directory)
	}

	return nil, fmt.Errorf("unsupported sagalog type: %s.  No sagalog created", config.Type)
}
