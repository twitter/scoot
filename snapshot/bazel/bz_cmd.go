package bazel

import (
	"os/exec"
)

type bzRunner interface {
	save(path string) ([]byte, error)                   // Called by bzFiler.Ingest
	materialize(sha string, dir string) ([]byte, error) // Called by bzFiler.Checkout and CheckoutAt
}

type bzCommand struct {
	localStorePath string
	// Not yet implemented:
	// bypassLocalStore bool
	// skipServer       bool
	// serverAddress    string
}

// Saves the file/dir specified by path using the fsUtilCmd & validates the id format
func (bc bzCommand) save(path string) ([]byte, error) {
	fileType, err := getFileType(path)
	if err != nil {
		return nil, err
	}

	output, err := bc.runCmd([]string{fileType, fsUtilCmdSave, path})
	if err != nil {
		return nil, err
	}

	err = validateID(string(output))
	if err != nil {
		return nil, err
	}

	return output, nil
}

// Materializes the digest identified by sha in dir using the fsUtilCmd
func (bc bzCommand) materialize(sha string, dir string) ([]byte, error) {
	return bc.runCmd([]string{fsUtilCmdDirectory, fsUtilCmdMaterialize, sha, dir})
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

func (bc noopBzRunner) save(path string) ([]byte, error)                   { return nil, nil }
func (bc noopBzRunner) materialize(sha string, dir string) ([]byte, error) { return nil, nil }
