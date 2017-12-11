package bazel

import (
	"os/exec"
	"strconv"
)

type bzRunner interface {
	save(path string) (string, error)         // Called by bzFiler.Ingest
	materialize(sha string, dir string) error // Called by bzFiler.Checkout and CheckoutAt
}

type bzCommand struct {
	localStorePath string
	// Not yet implemented:
	// bypassLocalStore bool
	// skipServer       bool
	// serverAddress    string
}

// Saves the file/dir specified by path using the fsUtilCmd & validates the id format
func (bc bzCommand) save(path string) (string, error) {
	fileType, err := getFileType(path)
	if err != nil {
		return "", err
	}

	output, err := bc.runCmd([]string{fileType, fsUtilCmdSave, path})
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
	if bc.localStorePath != "" {
		args = append(args, "--local-store-path", bc.localStorePath)
	}
	cmd := exec.Command(fsUtilCmd, args...)
	return cmd.Output()
}

// Noop bzRunner for stub testing
type noopBzRunner struct{}

func (bc noopBzRunner) save(path string) (string, error)         { return "", nil }
func (bc noopBzRunner) materialize(sha string, dir string) error { return nil }
