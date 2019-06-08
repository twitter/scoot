package bazel

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/common/dialer"
)

// Implements snapshot/bazel/bzTree
type bzCommand struct {
	localStorePath string
	casResolver    dialer.Resolver
	// Currently, uses resolver for each underlying runCmd. In the future, we may
	// want to limit the number of calls to Resolve if these happen too frequently.
	//
	// Not yet implemented:
	// bypassLocalStore bool
	// skipServer bool
}

func makeBzCommand(storePath string, resolver dialer.Resolver) bzCommand {
	return bzCommand{
		localStorePath: storePath,
		casResolver:    resolver,
	}
}

// Saves the file/dir specified by path using the fsUtilCmd & validates the id format
// Note: if there are any "irregular" files in path or path's parent dir (root) - e.g. *.sock
// files, etc. - fs_util will fail to expand globs.
func (bc bzCommand) save(path string) (string, error) {
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

	stdout, _, err := bc.runCmd(args)
	if err != nil {
		exitError, ok := err.(*exec.ExitError)
		if ok {
			return "", fmt.Errorf("Error: %s. Stderr: %s", err, exitError.Stderr)
		}
		return "", err
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
func (bc bzCommand) materialize(sha string, size int64, dir string) error {
	// short circuit if the input is empty, but create the target dir as fs_util would do
	if sha == bazel.EmptySha {
		return os.Mkdir(dir, 0777)
	}

	_, stderr, err := bc.runCmd([]string{fsUtilCmdDirectory, fsUtilCmdMaterialize, sha, strconv.FormatInt(size, 10), dir})
	if err != nil {
		exitError, ok := err.(*exec.ExitError)
		if ok {
			return &CheckoutNotExistError{Err: fmt.Sprintf("Error: %s. Stderr: %s", err, exitError.Stderr)}
		}
		return err
	}

	// temporarily dump stderr to program's stderr
	fmt.Fprintf(os.Stderr, "FS_UTIL MATERIALIZE DEBUG/STDERR:\n%s\n", string(stderr))

	return nil
}

// Runs fsUtilCmd as an os/exec.Cmd with appropriate flags
func (bc bzCommand) runCmd(args []string) ([]byte, []byte, error) {
	serverAddrs, err := bc.casResolver.ResolveAll()
	if err != nil {
		return nil, nil, err
	}

	// localStorePath required, add serverAddrs if resolved
	args = append([]string{fsUtilCmdLocalStore, bc.localStorePath}, args...)
	if len(serverAddrs) > 0 {
		for _, addr := range serverAddrs {
			args = append([]string{fsUtilCmdServerAddr, addr}, args...)
		}
	}

	log.Debugf("%s %s", fsUtilCmd, args)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := exec.Command(fsUtilCmd, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

// Noop bzTree for stub testing
type noopBzTree struct{}

func (bc noopBzTree) save(path string) (string, error)                     { return "", nil }
func (bc noopBzTree) materialize(sha string, size int64, dir string) error { return nil }
