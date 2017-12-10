package bazel

import (
	"os/exec"
)

type bzRunner interface {
	save(path string) ([]byte, error)
	materialize(sha string, dir string) ([]byte, error)
}

type bzCommand struct {
	command        string
	localStorePath string
	// Not yet implemented:
	// bypassLocalStore bool
	// skipServer       bool
	// serverAddress    string
}

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

func (bc bzCommand) materialize(sha string, dir string) ([]byte, error) {
	return bc.runCmd([]string{fsUtilCmdDirectory, fsUtilCmdMaterialize, sha, dir})
}

func (bc bzCommand) runCmd(args []string) ([]byte, error) {
	if bc.localStorePath != "" {
		args = append(args, "--local-store-path", bc.localStorePath)
	}
	cmd := exec.Command(bc.command, args...)
	return cmd.Output()
}

type noopBzCommand struct{}

func (bc noopBzCommand) save(path string) ([]byte, error) { return nil, nil }

func (bc noopBzCommand) materialize(sha string, dir string) ([]byte, error) { return nil, nil }
