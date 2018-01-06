package bazel

import (
	"os/exec"
	filepath "path"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/common"
)

type bzRunner interface {
	save(path string) (string, error)         // Called by bzFiler.Ingest
	materialize(sha string, dir string) error // Called by bzFiler.Checkout and CheckoutAt
}

type bzCommand struct {
	localStorePath string
	serverAddr     string
	// Not yet implemented:
	// bypassLocalStore bool
	// skipServer bool
}

func MakeBzCommand() bzCommand {
	return bzCommand{}
}

// Options is a variadic list of functions that take a *bzCommand as an arg
// and modify its fields, e.g.
// localStorePath := func(bc *bzCommand) {
//     bc.localStorePath = "/path/to/local/store"
// }
func MakeBzCommandWithOptions(options ...func(*bzCommand)) bzCommand {
	bc := bzCommand{}
	for _, opt := range options {
		opt(&bc)
	}
	return bc
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
	if bc.serverAddr != "" {
		args = append([]string{fsUtilCmdServerAddr, bc.serverAddr}, args...)
	}
	if bc.localStorePath != "" {
		args = append([]string{fsUtilCmdLocalStore, bc.localStorePath}, args...)
	}
	// We expect fs_util binary to be located at $GOPATH/bin, due to get_fs_util.sh
	gp, err := common.GetFirstGopath()
	if err != nil {
		return nil, err
	}
	return exec.Command(filepath.Join(gp, "bin", fsUtilCmd), args...).Output()
}

// Noop bzRunner for stub testing
type noopBzRunner struct{}

func (bc noopBzRunner) save(path string) (string, error)         { return "", nil }
func (bc noopBzRunner) materialize(sha string, dir string) error { return nil }
