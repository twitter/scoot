package conn

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/scootdev/scoot/daemon/protocol"
	"github.com/scootdev/scoot/runner"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// A Dialer can dial a connection to the Scoot server.
// It's useful to have this as a separate interface so you can wait to
// connect until you need the connection. This allows clients to do client-side
// only operations (e.g., printing help) without erroring if the server is down.
type Dialer interface {
	Dial() (Conn, error)
	io.Closer
}

type Conn interface {
	Echo(arg string) (string, error)

	runner.Controller
	runner.LegacyStatuses

	Close() error
}

func NewCachingDialer(dialer Dialer) Dialer {
	return &cachingDialer{dialer, nil}
}

type cachingDialer struct {
	dialer Dialer
	conn   Conn
}

func (d *cachingDialer) Dial() (Conn, error) {
	if d.conn == nil {
		conn, err := d.dialer.Dial()
		if err != nil {
			return nil, err
		}
		d.conn = conn
	}
	return d.conn, nil
}

func (d *cachingDialer) Close() error {
	if d.conn == nil {
		return nil
	}
	return d.conn.Close()
}

func UnixDialer() (Dialer, error) {
	socketPath, err := protocol.LocateSocket()
	if err != nil {
		return nil, err
	}
	return &dialer{socketPath}, nil
}

type dialer struct {
	socketPath string
}

func dial(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("unix", addr, timeout)
}

func (d *dialer) Dial() (Conn, error) {
	log.Println("Dialing ", d.socketPath)
	c, err := grpc.Dial(d.socketPath, grpc.WithInsecure(), grpc.WithDialer(dial))
	if err != nil {
		return nil, err
	}
	client := protocol.NewScootDaemonClient(c)
	return &conn{c, client}, nil
}

func (d *dialer) Close() error {
	return nil
}

type conn struct {
	conn   *grpc.ClientConn
	client protocol.ScootDaemonClient
}

func (c *conn) Echo(arg string) (string, error) {
	r, err := c.client.Echo(context.Background(), &protocol.EchoRequest{Ping: arg})
	if err != nil {
		return "", err
	}
	return r.Pong, nil
}

func (c *conn) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
	req := &protocol.RunRequest{Cmd: &protocol.RunRequest_Command{}}
	req.Cmd.Argv = cmd.Argv
	req.Cmd.Env = cmd.EnvVars
	req.Cmd.TimeoutNs = int64(cmd.Timeout)

	r, err := c.client.Run(context.Background(), req)
	if err != nil {
		return runner.ProcessStatus{}, err
	}
	if r.Error != "" {
		return runner.ProcessStatus{}, errors.New(r.Error)
	}
	return runner.RunningStatus(runner.RunId(r.RunId), "", ""), nil
}

func (c *conn) Status(run runner.RunId) (runner.ProcessStatus, error) {
	r, err := c.client.Poll(context.Background(), &protocol.PollRequest{RunIds: []string{string(run)}, All: true})
	if err != nil {
		return runner.ProcessStatus{}, err
	}
	if len(r.Status) != 1 {
		return runner.ProcessStatus{}, fmt.Errorf("Expected 1 status entry, got %v", len(r.Status))
	}
	return protocol.ToRunnerStatus(r.Status[0]), nil
}

func (c *conn) StatusAll() ([]runner.ProcessStatus, error) {
	panic("StatusAll not implemented in daemon code.")
}

func (c *conn) Abort(run runner.RunId) (runner.ProcessStatus, error) {
	panic("Abort not implemented in daemon code.")
}

func (c *conn) Erase(run runner.RunId) error {
	panic("Erase not implemented in daemon code.")
}

func (c *conn) Close() error {
	return c.conn.Close()
}
