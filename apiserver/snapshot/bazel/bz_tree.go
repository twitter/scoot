package bazel

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/worker/runner/execer"
	osexecer "github.com/twitter/scoot/worker/runner/execer/os"
)

// Implements snapshot/bazel/bzTree
type bzCommand struct {
	localStorePath string
	casResolver    dialer.Resolver

	// Not yet implemented:
	// bypassLocalStore bool
	// skipServer bool

	// Fields for managing underlying invocations of the fs_util tool
	execer  execer.Execer
	mu      sync.Mutex
	abortCh chan struct{}
}

func makeBzCommand(storePath string, resolver dialer.Resolver) *bzCommand {
	return &bzCommand{
		localStorePath: storePath,
		casResolver:    resolver,
		execer:         osexecer.NewExecer(),
	}
}

// Saves the file/dir specified by path using the fsUtilCmd & validates the id format
// Note: if there are any "irregular" files in path or path's parent dir (root) - e.g. *.sock
// files, etc. - fs_util will fail to expand globs.
func (bc *bzCommand) save(path string) (string, error) {
	fileType, err := getFileType(path)
	if err != nil {
		return "", err
	}
	args := []string{fileType, fsUtilCmdSave}

	// directory save requires root path
	if fileType == fsUtilCmdDirectory {
		args = append(args, fsUtilCmdRoot, path, fsUtilCmdGlobWildCard)
	} else {
		args = append(args, path)
	}
	log.Info(args)

	stdout, _, st, err := bc.runCmd(args)
	if err != nil {
		return "", fmt.Errorf("Error running command: %s", err)
	}
	if st.State != execer.COMPLETE || st.ExitCode != 0 || st.Error != "" {
		return "", fmt.Errorf("Error execing save. ProcessStatus: %v", st)
	}

	err = validateFsUtilSaveOutput(stdout)
	if err != nil {
		return "", err
	}

	s, err := splitFsUtilSaveOutput(stdout)
	if err != nil {
		return "", err
	}

	size, err := strconv.ParseInt(s[1], 10, 64)
	if err != nil {
		return "", err
	}

	id := bazel.SnapshotID(s[0], size)
	return id, nil
}

// Materializes the digest identified by sha in dir using the fsUtilCmd
func (bc *bzCommand) materialize(sha string, size int64, dir string) error {
	// short circuit if the input is empty, but create the target dir as fs_util would do
	if sha == bazel.EmptySha {
		return os.Mkdir(dir, 0777)
	}

	_, stderr, st, err := bc.runCmd([]string{fsUtilCmdDirectory, fsUtilCmdMaterialize, sha, strconv.FormatInt(size, 10), dir})
	if err != nil {
		return fmt.Errorf("Error running command: %s", err)
	}
	if st.State == execer.COMPLETE && st.ExitCode != 0 {
		// Interpret normal run with non-0 exit as "this didn't exist"
		return &CheckoutNotExistError{Err: fmt.Sprintf("Error -  ProcessStatus: %v", st)}
	} else if st.State != execer.COMPLETE || st.ExitCode != 0 || st.Error != "" {
		return fmt.Errorf("Error execing materialize. ProcessStatus: %v", st)
	}

	// temporarily dump stderr to program's stderr
	fmt.Fprintf(os.Stderr, "FS_UTIL MATERIALIZE DEBUG/STDERR:\n%s\n", string(stderr))

	return nil
}

// send a signal on the abortCh if there is an exec running
func (bc *bzCommand) cancel() error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if bc.abortCh != nil {
		bc.abortCh <- struct{}{}
	}
	return nil
}

// Runs fsUtilCmd via an execer with appropriate flags
func (bc *bzCommand) runCmd(args []string) ([]byte, []byte, execer.ProcessStatus, error) {
	// local store path required
	args = append([]string{fsUtilCmdLocalStore, bc.localStorePath}, args...)

	// set connection limit
	args = append([]string{fsUtilCmdConnLimit, fmt.Sprintf("%d", connLimit)}, args...)

	// add serverAddrs if we resolved any
	serverAddrs, err := bc.casResolver.ResolveMany(maxResolveToFSUtil)
	if err != nil {
		return nil, nil, execer.ProcessStatus{}, err
	}

	if len(serverAddrs) > 0 {
		for _, addr := range serverAddrs {
			args = append([]string{fsUtilCmdServerAddr, addr}, args...)
		}
		log.Debugf("%s %s", fsUtilCmd, args)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	argv := append([]string{fsUtilCmd}, args...)

	cmd := execer.Command{
		Argv:   argv,
		Stdout: &stdout,
		Stderr: &stderr,
	}

	st := bc.exec(cmd)

	return stdout.Bytes(), stderr.Bytes(), st, nil
}

func (bc *bzCommand) exec(c execer.Command) execer.ProcessStatus {
	bc.startupCh()
	p, err := bc.execer.Exec(c)

	if err != nil {
		return execer.ProcessStatus{
			State:    execer.FAILED,
			ExitCode: 0,
			Error:    fmt.Sprintf("failed to exec command: %s", err)}
	}

	processCh := make(chan execer.ProcessStatus, 1)
	go func() { processCh <- p.Wait() }()
	var st execer.ProcessStatus

	select {
	case st = <-processCh:
	case <-bc.abortCh:
		st = p.Abort()
	}

	bc.shutdownCh()
	return st
}

func (bc *bzCommand) startupCh() {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.abortCh = make(chan struct{})
}

func (bc *bzCommand) shutdownCh() {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	close(bc.abortCh)
	bc.abortCh = nil
}

// Noop bzTree for stub testing
type noopBzTree struct{}

func (bc *noopBzTree) save(path string) (string, error)                     { return "", nil }
func (bc *noopBzTree) materialize(sha string, size int64, dir string) error { return nil }
func (bc *noopBzTree) cancel() error                                        { return nil }
