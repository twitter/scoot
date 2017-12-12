package bazel

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"

	log "github.com/sirupsen/logrus"
)

type bzRunner interface {
	save(path string) (string, error)         // Called by bzFiler.Ingest
	materialize(sha string, dir string) error // Called by bzFiler.Checkout and CheckoutAt
}

type bzCommand struct {
	localStorePath string
	root           string
	serverAddr     string
	// Not yet implemented:
	// bypassLocalStore
	// skipServer
}

// Saves the file/dir specified by path using the fsUtilCmd & validates the id format
func (bc bzCommand) save(path string) (string, error) {
	fileType, err := getFileType(path)
	if err != nil {
		return "", err
	}
	args := []string{fileType, fsUtilCmdSave, path}
	if fileType == fsUtilCmdDirectory {
		args = append(args, "--root", bc.root)
	}
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

	id := generateId(s[0], size)
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
		args = append([]string{"--server-address", bc.serverAddr}, args...)
	}
	if bc.localStorePath != "" {
		args = append([]string{"--local-store-path", bc.localStorePath}, args...)
	}
	// We expect fs_util binary to be located at $GOPATH/bin, due to get_fs_util.sh
	gopath, ok := os.LookupEnv("GOPATH")
	if !ok {
		return nil, fmt.Errorf("Error: GOPATH not found in env")
	}
	// log.Error(fmt.Sprintf("%s/bin/%s", gopath, fsUtilCmd))
	log.Info(args)
	return exec.Command(fmt.Sprintf("%s/bin/%s", gopath, fsUtilCmd), args...).Output()
}

// Noop bzRunner for stub testing
type noopBzRunner struct{}

func (bc noopBzRunner) save(path string) (string, error)         { return "", nil }
func (bc noopBzRunner) materialize(sha string, dir string) error { return nil }
