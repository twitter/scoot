package bazel

import (
	"os/exec"
	filepath "path"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/common/dialer"
)

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
		base := filepath.Base(path)
		root := strings.TrimSuffix(path, base)
		args = append(args, fsUtilCmdRoot, root, filepath.Join(base, fsUtilCmdGlobWildCard))
	} else {
		args = append(args, path)
	}
	log.Info(args)

	output, err := bc.runCmd(args)
	if err != nil {
		return "", err
	}

	err = validateFsUtilSaveOutput(output)
	if err != nil {
		return "", err
	}

	s, err := splitFsUtilSaveOutput(output)
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
func (bc bzCommand) materialize(sha string, dir string) error {
	// we don't expect there to be any useful output
	_, err := bc.runCmd([]string{fsUtilCmdDirectory, fsUtilCmdMaterialize, sha, dir})
	return err
}

// Runs fsUtilCmd as an os/exec.Cmd with appropriate flags
func (bc bzCommand) runCmd(args []string) ([]byte, error) {
	serverAddr, err := bc.casResolver.Resolve()
	if err != nil {
		return nil, err
	}

	// localStorePath required, add serverAddr if resolved
	args = append([]string{fsUtilCmdLocalStore, bc.localStorePath}, args...)
	if serverAddr != "" {
		args = append([]string{fsUtilCmdServerAddr, serverAddr}, args...)
	}

	return exec.Command(fsUtilCmd, args...).Output()
}

// Noop bzRunner for stub testing
type noopBzRunner struct{}

func (bc noopBzRunner) save(path string) (string, error)         { return "", nil }
func (bc noopBzRunner) materialize(sha string, dir string) error { return nil }
