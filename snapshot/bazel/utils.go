package bazel

import (
	"fmt"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/bazel"
)

// Used as arg for fs_util binary
func getFileType(path string) (string, error) {
	var fileType string
	stat, err := os.Stat(path)
	if err != nil {
		log.Errorf("%s %s: %v", invalidFileTypeMsg, path, err)
		return fileType, err
	}

	switch mode := stat.Mode(); {
	case mode.IsDir():
		fileType = fsUtilCmdDirectory
	case mode.IsRegular():
		fileType = fsUtilCmdFile
	default:
		return "", fmt.Errorf("%s %s", invalidFileTypeMsg, path)
	}
	return fileType, err
}

// Checks that command line output from fs_util save is well formed
func validateFsUtilSaveOutput(output []byte) error {
	s, err := splitFsUtilSaveOutput(output)
	if err != nil {
		return err
	}
	sha, size := s[0], s[1]
	id := fmt.Sprintf("%s-%s-%s", bazel.SnapshotIDPrefix, sha, size)
	return bazel.ValidateID(id)
}

func splitFsUtilSaveOutput(output []byte) ([]string, error) {
	t := strings.TrimSpace(string(output))
	s := strings.Split(t, " ")
	if len(s) != 2 {
		return nil, fmt.Errorf("Error: %s. Expected <sha> <size>, received %v", invalidSaveOutputMsg, string(output))
	}
	return s, nil
}
