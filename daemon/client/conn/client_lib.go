package conn

// TODO renaming conn to client?
// this package provides an implementation of the client API for Scoot Daemon
// TODO - refactor run_cmd.go, echo_cmd.go and status_cmd.go to use Client
import (
	"fmt"
	"time"

	"github.com/scootdev/scoot/daemon/protocol"
	"github.com/scootdev/scoot/runner"
)

// this client lib uses Protobuf as the default transport.  Future extensions
// could add other transports.
func NewClientLib() Client {
	return &ClientLib{conn: makeProtobufConnection()}
}

// Client is the golang API for Scoot Daemon
type Client interface {
	Echo(echoValue string) (echo string, err error)
	CreateSnapshot(path string) (snapshotId string, err error)
	CheckoutSnapshot(checkoutId string, rootDir string) error
	Run(args []string, envVars map[string]string, timeout time.Duration, snapshotId string) (runId string, err error)
	Poll(runIds []string, timeout int64, all bool) (statuses []protocol.RunStatus, err error)
}

// ClientLib is an implemenation of Client.  A server must be started before using ClientLib.
// Both the Server and ClientLib require that the SCOOTDIR or HOME environment variables defined.
// If SCOOTDIR is defined, that will be the location used for the socket communications, otherwise
// the client and server will use $HOME/.scoot
type ClientLib struct {
	conn Conn
}

// The server will send echoValue back.  Use this function to verify
// the client's connection to the server.
func (c *ClientLib) Echo(echoValue string) (echo string, err error) {

	echo, err = c.conn.Echo(echoValue)

	return
}

// The server will preserve a snapshot of the files in path.
func (c *ClientLib) CreateSnapshot(path string) (snapshotId string, err error) {

	snapshotId, err = c.conn.CreateSnapshot(path)

	return
}

// The server will copy the files from the snapshot identified by snapshotId to
// rootDir.  If rootDir does not exist, it will be created.  If files from
// snapshotId overlap with files in rootDir, the files in rootDir will be
// overwritten with the files from snapshotId
func (c *ClientLib) CheckoutSnapshot(shapshotId string, rootDir string) error {

	return c.conn.CheckoutSnapshot(shapshotId, rootDir)
}

// question: should this return runId or ProcessStatus?
// Run a unix command after installing the snapshot and setting the environment
// variables from envVars.  Abort the run if it doesn't complete in timeout time.
func (c *ClientLib) Run(unixCommand []string, envVars map[string]string, timeout time.Duration, snapshotId string) (runId string, err error) {

	command := runner.NewCommand(unixCommand, envVars, timeout, snapshotId)
	process, err := c.conn.Run(command)
	if err != nil {
		return "", err
	}

	runId = string(process.RunId)
	err = fmt.Errorf(process.Error)

	return
}

// Poll for the status of runs identified by runIds.
// When timeout is < 0, Poll will wait indefinitely for at least 1 run to finish
// When timeout is == 0, Poll will return immediately with the statuses
// When timeout is > 0, Poll will wait at most timeout Nanoseconds for at least one run to complete
// When all is true - a status will be returned for each runid
// When all is false - the return will only contain the statuses of completed runs
func (c *ClientLib) Poll(runIds []string, timeout int64, all bool) (statuses []protocol.RunStatus, err error) {

	statuses, err = c.conn.Poll(runIds, timeout, all)

	return
}

func makeProtobufConnection() Conn {
	uDialer, err := UnixDialer()
	if err != nil {
		panic(fmt.Sprintf("Couldn't create UnixDialer:%s", err.Error()))
	}
	cDialer := NewCachingDialer(uDialer)
	conn, err := cDialer.Dial()

	if err != nil {
		panic(fmt.Sprintf("Error dialing: %s:", err.Error()))
	}

	return conn
}
