package conn

import (
	"fmt"
	"github.com/scootdev/scoot/local/protocol"
	"github.com/scootdev/scoot/runner"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

// A Dialer can dial a connection to the Scoot server.
// It's useful to have this as a separate interface so you can wait to
// connect until you need the connection. This allows clients to do client-side
// only operations (e.g., printing help) without erroring if the server is down.
type Dialer interface {
	Dial() (Conn, error)
}

type Conn interface {
	Echo(arg string) (string, error)

	// TODO(dbentley): this feels weird. We shouldn't expose our internal
	// API to the client. But it also feels weird to copy everything.
	// A Conn is also a Runner
	runner.Runner

	Close() error
}

type Comms interface {
	Conn
	Open() error
}

func NewComms(dialer Dialer) Comms {
	return &comms{dialer, nil}
}

type comms struct {
	dialer Dialer
	conn   Conn
}

func (c *comms) Open() error {
	if c.conn != nil {
		return nil
	}
	conn, err := c.dialer.Dial()
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *comms) Echo(arg string) (string, error) {
	if c.conn == nil {
		return "", fmt.Errorf("Comms is closed")
	}
	return c.conn.Echo(arg)
}

func (c *comms) Run(cmd *runner.Command) (*runner.ProcessStatus, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("Comms is closed")
	}
	return c.conn.Run(cmd)
}

func (c *comms) Status(run runner.RunId) (*runner.ProcessStatus, error) {
	if c.conn == nil {
		return nil, fmt.Errorf("Comms is closed")
	}
	return c.conn.Status(run)
}

func (c *comms) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
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
	client := protocol.NewLocalScootClient(c)
	return &conn{c, client}, nil
}

type conn struct {
	conn   *grpc.ClientConn
	client protocol.LocalScootClient
}

func (c *conn) Echo(arg string) (string, error) {
	r, err := c.client.Echo(context.Background(), &protocol.EchoRequest{Ping: arg})
	if err != nil {
		return "", err
	}
	return r.Pong, nil
}

func (c *conn) Run(cmd *runner.Command) (*runner.ProcessStatus, error) {
	req := &protocol.Command{}
	req.Argv = cmd.Argv
	req.Env = cmd.EnvVars
	req.Timeout = int64(cmd.Timeout)

	r, err := c.client.Run(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return protocol.ToRunnerStatus(r), nil
}

func (c *conn) Status(run runner.RunId) (*runner.ProcessStatus, error) {
	r, err := c.client.Status(context.Background(), &protocol.StatusQuery{RunId: string(run)})
	if err != nil {
		return nil, err
	}
	return protocol.ToRunnerStatus(r), nil
}

func (c *conn) Close() error {
	return c.conn.Close()
}
