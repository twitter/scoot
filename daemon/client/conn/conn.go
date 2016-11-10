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

	// TODO(dbentley): this feels weird. We shouldn't expose our internal
	// API to the client. But it also feels weird to copy everything.
	// A Conn is also a Runner
	runner.Runner

	CreateSnapshot(path string) (snapshotId string, err error)
	CheckoutSnapshot(checkoutId string, rootDir string) error
	Poll(runIds []string, timeout int64, all bool) (statuses []protocol.RunStatus, err error)

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
	Client protocol.ScootDaemonClient
}

func (c *conn) Echo(arg string) (string, error) {
	r, err := c.Client.Echo(context.Background(), &protocol.EchoRequest{Ping: arg})
	if err != nil {
		return "", err
	}
	return r.Pong, nil
}

func (c *conn) Run(cmd *runner.Command) (runner.ProcessStatus, error) {
	req := &protocol.RunRequest{Cmd: &protocol.RunRequest_Command{}}
	req.Cmd.Argv = cmd.Argv
	req.Cmd.Env = cmd.EnvVars
	req.Cmd.TimeoutNs = cmd.Timeout.Nanoseconds()
	req.Cmd.SnapshotId = cmd.SnapshotId

	r, err := c.Client.Run(context.Background(), req)
	if err != nil {
		return runner.ProcessStatus{}, err
	}
	if r.Error != "" {
		return runner.ProcessStatus{}, errors.New(r.Error)
	}
	return runner.RunningStatus(runner.RunId(r.RunId), "", ""), nil
}

func (c *conn) Status(run runner.RunId) (runner.ProcessStatus, error) {
	r, err := c.Client.Poll(context.Background(), &protocol.PollRequest{RunIds: []string{string(run)}, All: true})
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

func (c *conn) CreateSnapshot(path string) (snapshotId string, err error) {
	reply, err := c.Client.CreateSnapshot(context.Background(), &protocol.CreateSnapshotRequest{Path: path})
	if err != nil {
		return "", err
	}

	if len(reply.Error) != 0 {
		return "", fmt.Errorf(reply.Error)
	}

	return reply.SnapshotId, nil
}

func (c *conn) CheckoutSnapshot(checkoutId string, rootDir string) error {
	reply, err := c.Client.CheckoutSnapshot(context.Background(), &protocol.CheckoutSnapshotRequest{SnapshotId: checkoutId, Dir: rootDir})

	if err != nil {
		return err
	}

	if len(reply.Error) > 0 {
		return fmt.Errorf(reply.Error)
	}

	return nil
}

func (c *conn) Poll(runIds []string, timeout int64, all bool) (statuses []protocol.RunStatus, err error) {
	replies, err := c.Client.Poll(context.Background(), &protocol.PollRequest{RunIds: runIds, TimeoutNs: timeout, All: all})

	statuses = []protocol.RunStatus{}
	if err != nil {
		return statuses, err
	}

	for _, pbReply := range replies.Status {
		status := protocol.ToRunStatus(pbReply)
		statuses = append(statuses, status)
	}

	return
}
