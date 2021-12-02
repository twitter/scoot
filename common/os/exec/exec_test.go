package exec

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	osexec "os/exec"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	ExecMode = 0755
)

func removeAllSilently(path string) {
	_ = os.RemoveAll(path)
}

func closeAndRemoveSilently(f *os.File) {
	_ = os.RemoveAll(f.Name())
	_ = f.Close()
}

// creates a temporary executable containing data (i.e. a bash script)
// then calls cb with the file pointer of that script. The script is
// removed after cb returns. Any errors will fail the test.
func withTempExec(t *testing.T, data string, cb func(string)) {
	a := assert.New(t)
	fp, err := ioutil.TempFile("", "exec_test")
	a.NoError(err)
	defer fp.Close()
	defer removeAllSilently(fp.Name())

	i, err := fp.Write([]byte(data))
	a.NoError(err)
	a.Equal(len(data), i, "Did not write all bytes to tempfile, expected: %d, got: %d", len(data), i)

	a.NoError(fp.Chmod(ExecMode))

	// Close explicitly prior to cb() (in addition to the defer) to ensure we don't have the fd open when
	// when file is executed (to avoid EBUSY).
	a.NoError(fp.Close())

	cb(fp.Name())
}

func TestSplitEnvStrings(t *testing.T) {
	a := assert.New(t)

	var m map[string]string
	var err error

	m, err = splitEnvStrings([]string{"foo=bar", "baz=spam"})

	a.NoError(err)
	a.Equal("bar", m["foo"])
	a.Equal("spam", m["baz"])

	m, err = splitEnvStrings([]string{"bad"})
	a.Error(err)
	a.Nil(m)
}

func TestEnvGet(t *testing.T) {
	a := assert.New(t)

	env, err := NewEnv([]string{"foo=bar"})
	a.NoError(err)

	v, ok := env.Get("foo")
	a.True(ok)
	a.Equal("bar", v)

	x, ok := env.Get("xyz")
	a.False(ok)
	a.Equal("", x)
}

func TestEnvUnset(t *testing.T) {
	a := assert.New(t)

	envMap := make(map[string]string)
	envMap["foo"] = "bar"

	var env Env
	env = &processEnv{envMap: envMap}
	env.Unset("foo")

	_, ok := envMap["foo"]
	a.False(ok)
}

func TestWrapExitErrorNonExitError(t *testing.T) {
	a := assert.New(t)

	cmd := NewOsExec().Command("echo", "wat")

	myerr := errors.New("this is a unique error")
	ne := wrapExitError(cmd, myerr)

	a.Exactly(myerr, ne)
}

func TestSignalExitError(t *testing.T) {
	a := assert.New(t)
	ws := syscall.WaitStatus(15) // this is a signaled exit status

	// make sure our WaitStatus is correctly set up
	a.True(ws.Signaled())
	a.Equal(syscall.SIGTERM, ws.Signal())

	eea := &exitErrorAdapter{ws: ws}
	a.True(eea.Signaled())
	a.Equal(syscall.SIGTERM, eea.Signal())
	a.False(eea.Exited())
	a.Equal(-1, eea.ExitStatus())
}

func TestNonZeroExitError(t *testing.T) {
	a := assert.New(t)
	ws := syscall.WaitStatus(42 << 8) // this is an exit(42) status

	// make sure our WaitStatus is correctly set up
	a.True(ws.Exited())
	a.Equal(42, ws.ExitStatus())

	eea := &exitErrorAdapter{ws: ws}
	a.False(eea.Signaled())
	a.Equal(syscall.Signal(-1), eea.Signal())
	a.True(eea.Exited())
	a.Equal(42, eea.ExitStatus())
}

func TestCommandSimpleName(t *testing.T) {
	a := assert.New(t)
	oe := NewOsExec()

	expected, err := osexec.LookPath("echo") // this is a UNIX system...I know this...
	a.NoError(err)

	cmd := oe.Command("echo", "wat")
	a.Equal(expected, cmd.Path())

	args := cmd.Args()
	a.Equal([]string{"echo", "wat"}, args)

	// assert that modifying the args slice doesn't change the original
	args[0] = "noway"
	a.Equal([]string{"echo", "wat"}, cmd.Args())
}

func TestCommandAbsPath(t *testing.T) {
	a := assert.New(t)
	oe := NewOsExec()

	path := "/path/to/stuff"

	cmd := oe.Command(path, "wat")
	a.Equal(path, cmd.Path())

	args := cmd.Args()
	a.Equal([]string{path, "wat"}, args)
}

func TestRunSuccesss(t *testing.T) {
	a := assert.New(t)

	// TODO: implement dedent
	script := `#!/bin/bash
set -euo pipefail
echo "$@" > "${ARGV_OUTPUT}"
cat > "${STDIN_OUTPUT}"
echo "stdout"
echo "stderr" >&2
exit 0
`

	readString := func(r io.Reader) string {
		bytes, err := ioutil.ReadAll(r)
		a.NoError(err)
		return string(bytes)
	}

	withTempExec(t, script, func(fname string) {
		var err error

		argvfp, err := ioutil.TempFile("", "argvfile")
		a.NoError(err)
		defer closeAndRemoveSilently(argvfp)

		stdinfp, err := ioutil.TempFile("", "stdin")
		a.NoError(err)
		defer closeAndRemoveSilently(stdinfp)

		cmd := NewOsExec().Command(fname, "butts", "one", "two", "three")

		env, err := cmd.GetEnv()
		a.NoError(err)

		env.Set("ARGV_OUTPUT", argvfp.Name())
		env.Set("STDIN_OUTPUT", stdinfp.Name())

		cmd.SetEnv(env)

		cmd.SetStdin(strings.NewReader("stdin\n"))

		var stdoutbb bytes.Buffer
		var stderrbb bytes.Buffer

		cmd.SetStdout(bufio.NewWriter(&stdoutbb))
		cmd.SetStderr(bufio.NewWriter(&stderrbb))

		err = cmd.Run()
		a.NoError(err)

		a.Equal(
			[]string{"butts", "one", "two", "three"},
			strings.Split(strings.TrimSpace(readString(argvfp)), " "))

		a.Equal("stdin\n", readString(stdinfp))

		a.NotNil(cmd.Process())
		a.NotNil(cmd.ProcessState())

		a.Equal("stdout\n", stdoutbb.String())
		a.Equal("stderr\n", stderrbb.String())
	})
}

func TestStartWaitSuccess(t *testing.T) {
	a := assert.New(t)

	// TODO: implement dedent
	script := `#!/bin/bash
set -euo pipefail
echo "$@" > "${ARGV_OUTPUT}"
cat > "${STDIN_OUTPUT}"
echo "stdout"
echo "stderr" >&2
exit 0
`

	readString := func(r io.Reader) string {
		bytes, err := ioutil.ReadAll(r)
		a.NoError(err)
		return string(bytes)
	}

	withTempExec(t, script, func(fname string) {
		var err error

		argvfp, err := ioutil.TempFile("", "argvfile")
		a.NoError(err)
		defer closeAndRemoveSilently(argvfp)

		stdinfp, err := ioutil.TempFile("", "stdin")
		a.NoError(err)
		defer closeAndRemoveSilently(stdinfp)

		cmd := NewOsExec().Command(fname, "butts", "one", "two", "three")

		env, err := cmd.GetEnv()
		a.NoError(err)

		env.Set("ARGV_OUTPUT", argvfp.Name())
		env.Set("STDIN_OUTPUT", stdinfp.Name())

		cmd.SetEnv(env)

		cmd.SetStdin(strings.NewReader("stdin\n"))

		var stdoutbb bytes.Buffer
		var stderrbb bytes.Buffer

		cmd.SetStdout(bufio.NewWriter(&stdoutbb))
		cmd.SetStderr(bufio.NewWriter(&stderrbb))

		err = cmd.Start()
		a.NoError(err)

		err = cmd.Wait()
		a.NoError(err)

		a.Equal(
			[]string{"butts", "one", "two", "three"},
			strings.Split(strings.TrimSpace(readString(argvfp)), " "))

		a.Equal("stdin\n", readString(stdinfp))

		a.NotNil(cmd.Process())
		a.NotNil(cmd.ProcessState())

		a.Equal("stdout\n", stdoutbb.String())
		a.Equal("stderr\n", stderrbb.String())
	})
}

func TestCmdReturnsArgsCopy(t *testing.T) {
	a := assert.New(t)
	cmd := NewOsExec().Command("echo", "butts", "one", "two", "three")
	args := cmd.Args()
	args[1] = "xyz"

	a.Equal("butts", cmd.Args()[1])
}

func TestOuputSuccess(t *testing.T) {
	a := assert.New(t)

	script := `#!/bin/bash
echo "stdout"
`

	withTempExec(t, script, func(fname string) {
		cmd := NewOsExec().Command(fname)
		outbytes, err := cmd.Output()
		a.NoError(err)
		a.Equal("stdout\n", string(outbytes))
	})
}

func TestOutputFailure(t *testing.T) {
	a := assert.New(t)

	script := `#!/bin/bash
echo "stdout"
exit 42
`
	withTempExec(t, script, func(fname string) {
		cmd := NewOsExec().Command(fname)
		outbytes, err := cmd.Output()
		a.Error(err)
		a.Equal("stdout\n", string(outbytes))
	})
}

func TestRunFailureExitNonZero(t *testing.T) {
	a := assert.New(t)

	script := `#!/bin/bash
exit 42
`
	withTempExec(t, script, func(fname string) {
		cmd := NewOsExec().Command(fname, "butts")
		err := cmd.Run()

		a.Error(err)

		ee, ok := err.(ExitError)
		a.True(ok)
		a.Equal(fname, ee.Path())
		a.Equal([]string{fname, "butts"}, ee.Args())
		a.True(ee.Exited())
		a.Equal(42, ee.ExitStatus())
		a.False(ee.Signaled())
		a.Equal(syscall.Signal(-1), ee.Signal())
		a.NotNil(cmd.Process())
		a.NotNil(cmd.ProcessState())

		// this is cheating, but we *are* delegating
		eea, ok := ee.(*exitErrorAdapter)
		a.True(ok)
		a.EqualError(ee, eea.err.Error())
		a.Equal(cmd.ProcessState().Pid(), ee.Pid())
	})
}
