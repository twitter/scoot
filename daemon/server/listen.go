package server

import (
	"github.com/scootdev/scoot/daemon/protocol"
	"net"
	"os"
	"path"
	"syscall"
)

func Listen() (net.Listener, error) {
	socketPath, err := protocol.LocateSocket()
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(path.Dir(socketPath), 0700)
	if err != nil {
		return nil, err
	}
	l, err := net.Listen("unix", socketPath)
	if err == nil {
		return l, nil
	}

	l = replaceDeadServer(socketPath, err)
	if l != nil {
		return l, nil
	}
	return nil, err
}

// replaceDeadServer handles the common case where a daemon has died but the socket file still exists.
// If the address is already in use, we try connecting to it.
// If we get connection refused, we infer the server is dead and
// remove the socket file, and then try serving.
// Returns a valid listener or nil.
func replaceDeadServer(socketPath string, err error) net.Listener {
	if !isAddrAlreadyInUse(err) {
		return nil
	}

	conn, connErr := net.Dial("unix", socketPath)
	if connErr == nil {
		// There is an active server, so bow out gracefully
		conn.Close()
		return nil
	}
	if !isConnectionRefused(connErr) {
		return nil
	}
	pathErr := os.Remove(socketPath)
	if pathErr != nil {
		return nil
	}
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil
	}
	return l
}

func isAddrAlreadyInUse(err error) bool {
	opErr, ok := err.(*net.OpError)
	if !ok {
		return false
	}
	sysErr, ok := opErr.Err.(*os.SyscallError)
	if !ok {
		return false
	}
	errno, ok := sysErr.Err.(syscall.Errno)
	if !ok {
		return false
	}
	return errno == syscall.EADDRINUSE
}

func isConnectionRefused(err error) bool {
	opErr, ok := err.(*net.OpError)
	if !ok {
		return false
	}
	sysErr, ok := opErr.Err.(*os.SyscallError)
	if !ok {
		return false
	}
	errno, ok := sysErr.Err.(syscall.Errno)
	if !ok {
		return false
	}
	return errno == syscall.ECONNREFUSED
}
